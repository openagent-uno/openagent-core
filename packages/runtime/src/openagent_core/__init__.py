"""OpenAgent's dependency-free embedding API.

Optional engine, provider, gateway and storage implementations are imported only
when selected by the host. Importing this package performs no I/O.
"""
from .contracts import (PrincipalRef, ExecutionContext, CapabilityLease, ResourceRef,
    RunRequest, RunRecord, RunEvent, Authorizer, IdentityDirectory, RuntimeStore,
    ModelCatalog, CredentialResolver, DelegationService, IdempotencyConflict)
from .runtime import Runtime, RuntimeSettings, RuntimeServices
from .capabilities import (CapabilityCatalog, CapabilitySource, ToolExecutor,
    ToolDefinition, ToolDescriptor, FunctionSource, CapabilityUnavailable)
from .extensions import EngineExtensions

__version__ = '1.0.0b1'
__all__ = ['Runtime','RuntimeSettings','RuntimeServices','PrincipalRef','ExecutionContext',
    'CapabilityLease','ResourceRef','RunRequest','RunRecord','RunEvent','Authorizer',
    'IdentityDirectory','RuntimeStore','ModelCatalog','CredentialResolver','DelegationService',
    'CapabilityCatalog','CapabilitySource','ToolExecutor','ToolDefinition','ToolDescriptor',
    'FunctionSource','CapabilityUnavailable','IdempotencyConflict','EngineExtensions']
