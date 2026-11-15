from pathlib import Path

from openagent_core.module_adapters import NativeCapabilityDescriptor

def module_assets(name: str = "vault") -> Path:
    if name != "vault":
        raise LookupError(f"Unknown vault module resources: {name}")
    path = Path(__file__).resolve().parent / "resources" / "vault"
    if not (path / "manifest.json").is_file():
        raise FileNotFoundError(f"Vault resources are missing from {path}")
    return path

descriptor=NativeCapabilityDescriptor(id="vault",version="1.1.0b1",
    native_sources=("vault","vault-gate"),source_names={"vault":"vault","vault-gate":"vault-gate"},
    search_operation="vault_search",search_source="vault-gate",search_result_key="results",
    search_query_argument="query",
    runtime_environment={"OPENAGENT_MODULE_ASSETS_VAULT": str(module_assets())},
    # Memory rules, in section order: where memory lives (and that a backend's
    # own memory is ignored), the non-negotiable read/save rules, what not to
    # save and note quality, and the end-of-turn check.
    prompt_rule_ids=("module.vault.storage","module.vault.discipline",
                     "module.vault.quality","module.vault.checklist"))
__all__=["descriptor","module_assets"]
