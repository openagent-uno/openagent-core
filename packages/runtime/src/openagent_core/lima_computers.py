"""Optional single-hypervisor computer provider using private Lima instances."""

from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager, contextmanager
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import tempfile
import time
from uuid import UUID

import yaml

from ._guest_channel import GuestChannel
from .capabilities import CapabilityUnavailable
from .computer_tasks import ComputerConnection
from .contracts import canonical_json


_NAME = re.compile(r"[a-z][a-z0-9-]{0,46}")
_KEY = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"(?:[a-f0-9]{40}|[a-f0-9]{64})")
_CAP = 16 * 1024 * 1024


def _save(path, payload):
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    os.replace(output.name, path)
    with _directory_fd(path.parent) as directory:
        os.fsync(directory)


@contextmanager
def _directory_fd(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        yield fd
    finally:
        os.close(fd)


async def _command(argv, *, env, data=b"", limit=_CAP, timeout=180):
    proc = await asyncio.create_subprocess_exec(
        *argv,
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )

    async def write():
        try:
            proc.stdin.write(data)
            await proc.stdin.drain()
        finally:
            proc.stdin.close()

    async def read():
        out = bytearray()
        while chunk := await proc.stdout.read(65536):
            out.extend(chunk)
            if len(out) > limit:
                raise ValueError("Computer response exceeds its size limit")
        return bytes(out)

    try:
        async with asyncio.timeout(timeout):
            _, result = await asyncio.gather(write(), read())
            if await proc.wait():
                raise CapabilityUnavailable("The exact task computer operation failed")
            return result
    finally:
        if proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()


class LimaComputerProvider:
    """Local virtualization control plane. Bind one durable private Lima home to one provider."""

    def __init__(
        self, home, bases, *, lease_seconds=60, executable="limactl", ssh="ssh", egress_proxy=None
    ):
        self.home = Path(home).resolve(strict=True)
        if self.home.stat().st_uid != os.getuid() or self.home.stat().st_mode & 0o077:
            raise PermissionError("The Lima provider requires a private operator-owned directory")
        # (project, version) -> (sealed stopped base VM, immutable project Git commit).
        self.bases = dict(bases)
        for (project, version), (base, commit) in self.bases.items():
            if not project or not version or not _NAME.fullmatch(base) or not _COMMIT.fullmatch(commit):
                raise ValueError("Invalid sealed Lima base or Git commit")
        if not 0 < lease_seconds <= 300:
            raise ValueError("Invalid computer heartbeat duration")
        self.lease_seconds, self.executable, self.ssh = lease_seconds, executable, ssh
        self.egress_rule = ""
        if egress_proxy is not None:
            address, port = egress_proxy
            ip = ipaddress.ip_address(address)
            if type(port) is not int or not 1 <= port <= 65535:
                raise ValueError("Expected a fixed proxy port")
            family = "ip6" if ip.version == 6 else "ip"
            self.egress_rule = f"meta skuid $worker {family} daddr {ip} tcp dport {port} accept"
        self.private = self.home / "_computer_tasks"
        self.private.mkdir(mode=0o700, exist_ok=True)
        self.env = {**os.environ, "LIMA_HOME": str(self.home)}
        self.broker = None
        self._channels = {}
        self._opening = asyncio.Lock()

    def bind(self, broker):
        if broker.provider is not self or broker.transport is not self:
            raise ValueError("The controller must own both provisioner and transport")
        self.broker = broker

    def _dir(self, key):
        if not isinstance(key, str) or not _KEY.fullmatch(key):
            raise ValueError("Invalid allocation")
        return self.private / key

    @asynccontextmanager
    async def _locked(self, key):
        directory = self._dir(key)
        directory.mkdir(mode=0o700, exist_ok=True)
        with (directory / "lock").open("a+b") as lock:
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await asyncio.sleep(0.1)
            yield directory

    def _machine(self, key):
        self._dir(key)
        return "oa-" + base64.b32encode(bytes.fromhex(key)[:20]).decode().lower()

    async def _lima(self, *args):
        return await _command((self.executable, "-y", *args), env=self.env, timeout=300)

    async def _status(self, name):
        if not (self.home / name / "lima.yaml").exists():
            return "Absent"
        row = json.loads(await self._lima("list", "--format", "json", name))
        if row.get("name") != name:
            raise CapabilityUnavailable("Lima inventory changed")
        return row["status"]

    def _ssh_argv(self, key, command):
        name = self._machine(key)
        return (
            self.ssh,
            "-F",
            str(self.home / name / "ssh.config"),
            "-o",
            "BatchMode=yes",
            "-o",
            "ForwardAgent=no",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "ControlMaster=no",
            "-o",
            "ControlPath=none",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "GlobalKnownHostsFile=/dev/null",
            "-o",
            "HostKeyAlgorithms=ssh-ed25519",
            "-o",
            "HostKeyAlias=" + name,
            "-o",
            "UserKnownHostsFile=" + str(self._dir(key) / "known_hosts"),
            "lima-" + name,
            command,
        )

    async def _ssh(self, key, command, *, data=b"", limit=_CAP, timeout=120):
        return await _command(
            self._ssh_argv(key, command), env=self.env, data=data, limit=limit, timeout=timeout
        )

    async def _pin(self, directory, name):
        private = directory / "guest_host_key"
        if not private.exists():
            await _command(
                ("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(private)), env=self.env
            )
        public = private.with_suffix(".pub").read_text().split()[:2]
        _save(directory / "known_hosts", (name + " " + " ".join(public) + "\n").encode())
        path = self.home / name / "lima.yaml"
        conf = yaml.safe_load(path.read_bytes())
        conf.update(
            plain=True,
            mounts=[],
            additionalDisks=[],
            containerd={"system": False, "user": False},
            ssh={**(conf.get("ssh") or {}), "forwardAgent": False, "loadDotSSHPubKeys": False},
        )
        content = base64.b64encode(private.read_bytes()).decode()
        firewall = f"""#!/bin/sh
set -eu
umask 077
worker=$(id -u oawork)
test "$worker" -gt 0
if sudo -n -u oawork sudo -n true 2>/dev/null; then exit 1; fi
delete=
if nft list table inet oa_task_egress >/dev/null 2>&1; then delete='delete table inet oa_task_egress'; fi
nft -f - <<RULES
$delete
table inet oa_task_egress {{
 chain output {{
  type filter hook output priority 0; policy accept;
  meta skuid $worker oifname "lo" accept
  {self.egress_rule}
  meta skuid $worker drop
 }}
}}
RULES
"""
        script = f"""#!/bin/sh
set -eu
umask 077
nft list table inet oa_task_egress >/dev/null
printf %s '{content}' | base64 -d >/etc/ssh/ssh_host_ed25519_key
ssh-keygen -y -f /etc/ssh/ssh_host_ed25519_key >/etc/ssh/ssh_host_ed25519_key.pub
if [ -s /run/sshd.pid ]; then kill -HUP "$(cat /run/sshd.pid)"; fi
"""
        conf["provision"] = [
            *(conf.get("provision") or []),
            {"mode": "boot", "script": firewall},
            {"mode": "system", "script": script},
        ]
        _save(path, yaml.safe_dump(conf).encode())
        _save(directory / "configured", b"1")

    async def ensure(self, allocation):
        key, name = allocation.key, self._machine(allocation.key)
        spec = self.bases.get((allocation.task.project, allocation.task.base_version))
        if spec is None:
            raise PermissionError("Unknown project snapshot")
        async with self._locked(key) as directory:
            state = directory / "reservation.json"
            identity = {
                "key": key,
                "project": allocation.task.project,
                "version": allocation.task.base_version,
                "base": spec[0],
                "commit": spec[1],
                "machine": name,
            }
            if state.exists() and json.loads(state.read_bytes()) != identity:
                raise CapabilityUnavailable("Existing reservation changed")
            if (directory / "revoked").exists():
                raise PermissionError("Revoked VM reservation")
            if not state.exists():
                if await self._status(name) != "Absent":
                    raise CapabilityUnavailable("Instance name is already occupied")
                _save(state, canonical_json(identity).encode())
            if await self._status(name) == "Absent":
                if await self._status(spec[0]) != "Stopped":
                    raise CapabilityUnavailable("A sealed stopped base VM is required")
                await self._lima("clone", spec[0], name, "--mount-none")
            if not (directory / "configured").exists():
                if await self._status(name) != "Stopped":
                    raise CapabilityUnavailable("Unconfigured clone must be stopped")
                await self._pin(directory, name)
            return name

    async def connection(self, allocation, machine_id):
        key = allocation.key
        if self._machine(key) != machine_id:
            raise CapabilityUnavailable("Incorrect VM enrollment")
        async with self._locked(key) as directory:
            if (directory / "revoked").exists() or not (directory / "configured").exists():
                raise CapabilityUnavailable("VM enrollment unavailable")
            status = await self._status(machine_id)
            if status == "Stopped":
                await self._lima("start", machine_id)
            elif status != "Running":
                raise CapabilityUnavailable("VM is not ready")
            boot = str(
                UUID(
                    (
                        await self._ssh(
                            key,
                            "sudo -n nft list table inet oa_task_egress >/dev/null && "
                            "cat /proc/sys/kernel/random/boot_id",
                            limit=128,
                        )
                    )
                    .decode()
                    .strip()
                )
            )
            if not (directory / "verified-base").exists():
                expected = json.loads((directory / "reservation.json").read_bytes())["commit"]
                observed = await self._ssh(
                    key, "sudo -n -u oawork git -C /srv/workspace rev-parse HEAD", limit=128
                )
                if observed.decode().strip() != expected:
                    raise CapabilityUnavailable("The cloned workspace does not contain the approved base")
                _save(directory / "verified-base", expected.encode())
            path = directory / "generation.json"
            prior = json.loads(path.read_bytes()) if path.exists() else {"boot": None, "generation": 0}
            if prior["boot"] != boot:
                prior = {"boot": boot, "generation": prior["generation"] + 1}
                _save(path, canonical_json(prior).encode())
            return ComputerConnection(
                machine_id, machine_id, prior["generation"], time.time() + self.lease_seconds
            )

    async def close(self):
        """Release control connections while leaving VM disks and power state under host control."""
        await asyncio.gather(*(channel.close() for channel in tuple(self._channels.values())))

    async def suspend(self, allocation):
        async with self._locked(allocation.key):
            if channel := self._channels.get(allocation.key):
                await channel.close()
            name = self._machine(allocation.key)
            if await self._status(name) == "Running":
                await self._lima("stop", name)

    async def fence(self, allocation):
        async with self._locked(allocation.key) as directory:
            _save(directory / "revoked", b"1")
            name = self._machine(allocation.key)
            if await self._status(name) == "Running":
                await self._lima("stop", name)

    async def _entry(self, context, selectors):
        if self.broker is None:
            raise CapabilityUnavailable("Computer provider has no bound access controller")
        lease = await self.broker.admit_target(
            context, selectors.get("host_id"), selectors.get("generation")
        )
        state = self._dir(lease.allocation)
        if (state / "revoked").exists() or not (state / "generation.json").is_file():
            raise CapabilityUnavailable("VM enrollment unavailable")
        enrollment = json.loads((state / "generation.json").read_bytes())
        if enrollment["generation"] != lease.generation or self._machine(lease.allocation) != lease.host_id:
            raise CapabilityUnavailable("VM enrollment changed")
        return lease, enrollment["boot"]

    async def _rpc(self, context, scope, body):
        lease, boot = await self._entry(context, scope)
        if body["type"] in {"call", "read_resource"}:
            # The device protocol expects a certified account, network and
            # instance. Only the trusted context and enrollment supply these.
            body = {
                **body,
                "principal": {
                    "kind": context.initiator.kind,
                    "account_id": hashlib.sha256(context.initiator.key.encode()).hexdigest(),
                    "network_id": hashlib.sha256(
                        canonical_json([context.initiator.authority, context.tenant_id]).encode()
                    ).hexdigest(),
                    "client_instance_id": lease.host_id,
                    "device_id": lease.machine_id,
                    "generation": lease.generation,
                    "browser_scope": lease.allocation,
                },
            }
        # The pinned SSH host key verifies the machine. A check within this
        # same session rejects a reboot before passing any input to the broker.
        command = (
            'test "$(cat /proc/sys/kernel/random/boot_id)" = '
            + shlex.quote(boot)
            + " && exec sudo -n -u oawork /usr/local/bin/oa-computer-stdio"
        )
        key = lease.allocation
        identity = (boot, lease.epoch)
        async with self._opening:
            channel = self._channels.get(key)
            if channel is not None and (channel.identity != identity or channel.proc.stdin.is_closing()):
                await channel.close()
                channel = None
            if channel is None:

                def remove(closed):
                    if self._channels.get(key) is closed:
                        self._channels.pop(key)

                channel = await GuestChannel.open(
                    self._ssh_argv(key, command),
                    self.env,
                    self._dir(key),
                    identity,
                    self.lease_seconds,
                    remove,
                    _CAP,
                )
                self._channels[key] = channel
        return await channel.request(body)

    async def catalog(self, context, selectors):
        result = await self._rpc(context, selectors, {"type": "catalog"})
        rows = []
        for manifest in result["servers"]:
            name = manifest["name"]
            if selectors.get("server", name) == name:
                rows.append(
                    {
                        "host_id": selectors["host_id"],
                        "generation": selectors["generation"],
                        "server": name,
                        "version": manifest["version"],
                        "tools": manifest["tools"],
                        "catalog_digest": hashlib.sha256(canonical_json(manifest).encode()).hexdigest(),
                        "resource_protocol": "mcp-app-resources/1",
                    }
                )
        return {"servers": rows}

    async def _exact(self, context, scope):
        rows = (await self.catalog(context, scope))["servers"]
        if len(rows) != 1 or rows[0]["catalog_digest"] != scope.get("catalog_digest"):
            raise CapabilityUnavailable("The exact remote catalog changed")

    async def call(self, context, scope, name, arguments):
        await self._exact(context, scope)
        return await self._rpc(
            context,
            scope,
            {
                "type": "call",
                "server": scope["server"],
                "tool": name,
                "args": arguments,
            },
        )

    async def read(self, context, scope, uri):
        await self._exact(context, scope)
        return await self._rpc(
            context,
            scope,
            {
                "type": "read_resource",
                "server": scope["server"],
                "uri": uri,
            },
        )

    async def checkpoint(self, lease, request_id):
        key = lease.allocation
        async with self._locked(key) as directory:
            name = hashlib.sha256(request_id.encode()).hexdigest()
            path = directory / ("checkpoint-" + name)
            if not path.exists():
                if (directory / "revoked").exists():
                    raise PermissionError("The VM was revoked")
                commit = json.loads((directory / "reservation.json").read_bytes())["commit"]
                generation = json.loads((directory / "generation.json").read_bytes())
                if generation["generation"] != lease.generation:
                    raise CapabilityUnavailable("The VM boot changed")
                # Git builds a temporary index, including tracked deletions and
                # untracked additions. No branch, user index or base is changed.
                script = """set -eu
cd /srv/workspace
d=$(mktemp -d)
trap 'rm -rf "$d"' EXIT
export GIT_INDEX_FILE="$d/index" GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null
git -c core.fsmonitor=false read-tree "$1"
git -c core.fsmonitor=false -c core.hooksPath=/dev/null add -A -- .
tree=$(git -c core.fsmonitor=false write-tree)
printf '%s\n' "$tree"
git -c core.fsmonitor=false diff --binary --full-index --no-ext-diff --no-textconv "$1" "$tree" --
"""
                cmd = (
                    'test "$(cat /proc/sys/kernel/random/boot_id)" = '
                    + shlex.quote(generation["boot"])
                    + " && exec sudo -n -u oawork sh -s -- "
                    + shlex.quote(commit)
                )
                raw = await self._ssh(key, cmd, data=script.encode(), limit=8 * 1024 * 1024)
                tree, _, patch = raw.partition(b"\n")
                if not _COMMIT.fullmatch(tree.decode("ascii")):
                    raise ValueError("The guest returned an invalid Git tree")
                receipt = {
                    "allocation": key,
                    "request": name,
                    "base_commit": commit,
                    "tree": tree.decode("ascii"),
                    "epoch": lease.epoch,
                    "generation": lease.generation,
                    "sha256": hashlib.sha256(patch).hexdigest(),
                    "bytes": len(patch),
                }
                _save(
                    path,
                    canonical_json({"receipt": receipt, "diff": base64.b64encode(patch).decode()}).encode(),
                )
            return json.loads(path.read_bytes())["receipt"]

    async def read_checkpoint(self, allocation, request_id):
        name = hashlib.sha256(request_id.encode()).hexdigest()
        path = self._dir(allocation.key) / ("checkpoint-" + name)
        if not path.is_file():
            raise LookupError("Unknown computer checkpoint")
        value = json.loads(path.read_bytes())
        patch = base64.b64decode(value["diff"], validate=True)
        if hashlib.sha256(patch).hexdigest() != value["receipt"]["sha256"]:
            raise CapabilityUnavailable("Computer checkpoint integrity failure")
        return value["receipt"], patch
