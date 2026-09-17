from pathlib import Path

from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.prompts import PromptBlock, rule_blocks

_discipline=rule_blocks("module.vault.discipline")[0]
_start=_discipline.text.index("- **Curated memory is not conversation history.**")
_end=_discipline.text.index("- **AFTER any learning**",_start)
VAULT_DISCIPLINE=PromptBlock("module.vault.discipline","1.1",
    _discipline.text[:_start]+_discipline.text[_end:],"openagent-module-vault")

_history=rule_blocks("module.vault.history")[0]
_isolation_start=_history.text.index("CRITICAL —")
VAULT_BACKEND_ISOLATION=PromptBlock("module.vault.backend-isolation","1.1",
    _history.text[_isolation_start:],"openagent-module-vault")

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
    prompt_rule_ids=("module.vault.storage","module.vault.quality","module.vault.checklist"),
    additional_prompt_blocks=(VAULT_DISCIPLINE,VAULT_BACKEND_ISOLATION))
__all__=["descriptor","module_assets"]
