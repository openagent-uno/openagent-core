#!/usr/bin/env python3
"""Build wheels from one clean source snapshot, never a stale build/lib tree."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import zipfile
from email.parser import BytesParser

PACKAGES = ('.','packages/storage-sqlite','packages/modules','packages/capability-host',
            'packages/gateway','sdk/python')


def command(*args,cwd):
    return subprocess.check_output(args,cwd=cwd).decode().strip()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',type=Path,required=True)
    options=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    output=options.out.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.glob('*.whl')):
        parser.error('Use an empty output directory to avoid mixing source snapshots')
    if shutil.which('uv') is None:
        parser.error('Install the uv build frontend before building')
    revision=command('git','rev-parse','HEAD',cwd=root)
    dirty=bool(command('git','status','--porcelain',cwd=root))
    names=subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=root).decode().split('\0')
    sources={}
    with tempfile.TemporaryDirectory(prefix='openagent-core-build-') as temporary:
        snapshot=Path(temporary)/'source';snapshot.mkdir()
        for name in sorted(set(names)-{''}):
            source=root/name
            if not source.is_file():continue
            if source.is_symlink():raise ValueError('Source snapshot cannot follow symbolic links: '+name)
            data=source.read_bytes();sources[name]=hashlib.sha256(data).hexdigest()
            destination=snapshot/name;destination.parent.mkdir(parents=True,exist_ok=True)
            destination.write_bytes(data);shutil.copymode(source,destination)
        for name in PACKAGES:
            result=subprocess.run(['uv','build','--wheel','--out-dir',str(output),str(snapshot/name)],
                text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            if result.returncode:
                raise RuntimeError(f'Build failed for {name}:\n'+result.stdout)
            print('Built '+name,flush=True)
    wheels=[]
    for wheel in sorted(output.glob('*.whl')):
        with zipfile.ZipFile(wheel) as archive:
            metadata=BytesParser().parsebytes(archive.read(next(name for name in archive.namelist() if name.endswith('.dist-info/METADATA'))))
        wheels.append({'file':wheel.name,'name':metadata['Name'],'version':metadata['Version'],
            'sha256':hashlib.sha256(wheel.read_bytes()).hexdigest(),'bytes':wheel.stat().st_size})
    source_digest=hashlib.sha256(json.dumps(sources,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    manifest={'format':1,'repository_commit':revision,'dirty_snapshot':dirty,'source_sha256':source_digest,
              'sources':sources,'wheels':wheels}
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print('Manifest: '+str(output/'manifest.json'))


if __name__=='__main__':main()
