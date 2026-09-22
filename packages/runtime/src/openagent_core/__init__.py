"""OpenAgent's dependency-free embedding API.

Optional engine, provider, gateway and storage implementations are imported only
when selected by the host. Importing this package performs no I/O.
"""
from .contracts import (PrincipalRef, ExecutionContext, CapabilityLease, ResourceRef,
    RunRequest, AcceptedRunRequest, RunRecord, RunEvent, Authorizer, IdentityDirectory, RuntimeStore,
    ModelCatalog, CredentialResolver, DelegationService, IdempotencyConflict, SessionRef, SessionStore)
from .runtime import Runtime, RuntimeSettings, RuntimeServices
from .capabilities import (CapabilityCatalog, CapabilitySource, ToolExecutor,
    ToolDefinition, ToolDescriptor, FunctionSource, CapabilityUnavailable)
from .extensions import EngineExtensions
from .modules import (MODULE_API_VERSION, MODULE_SURFACES, CapabilityContribution,
    ModuleCatalog, ModuleConfig, ModuleContext, ModuleContribution, ModuleDescriptor,
    ModuleInstance, ModuleMigration, ModuleReconfigurationError, ModuleReferenceConflict,
    ModuleReferenceInspector, ModuleResolutionError,
    ModuleStatus, ReconfigureReceipt, RuntimeProfile, ServiceBinding, ServiceRegistry)

__version__ = '1.1.0b3'
__all__ = ['Runtime','RuntimeSettings','RuntimeServices','PrincipalRef','ExecutionContext',
    'CapabilityLease','ResourceRef','RunRequest','AcceptedRunRequest','RunRecord','RunEvent','Authorizer',
    'IdentityDirectory','RuntimeStore','SessionRef','SessionStore','ModelCatalog','CredentialResolver','DelegationService',
    'CapabilityCatalog','CapabilitySource','ToolExecutor','ToolDefinition','ToolDescriptor',
    'FunctionSource','CapabilityUnavailable','IdempotencyConflict','EngineExtensions',
    'MODULE_API_VERSION','MODULE_SURFACES','CapabilityContribution','ModuleCatalog','ModuleConfig',
    'ModuleContext','ModuleContribution','ModuleDescriptor','ModuleInstance','ModuleMigration',
    'ModuleReconfigurationError','ModuleReferenceConflict','ModuleReferenceInspector',
    'ModuleResolutionError','ModuleStatus','ReconfigureReceipt',
    'RuntimeProfile','ServiceBinding','ServiceRegistry']
