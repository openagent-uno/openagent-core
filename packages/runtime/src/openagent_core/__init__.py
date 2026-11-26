"""OpenAgent's dependency-free embedding API.

Optional engine, provider, gateway and storage implementations are imported only
when selected by the host. Importing this package performs no I/O.
"""
from .contracts import (PrincipalRef, ExecutionContext, CapabilityLease, ResourceRef,
    AutomationRunKind, AutomationRunCancellationStore,
    RunRequest, RunAdmissionReadiness, RunAdmissionPending, WorkerLeaseExpired, AcceptedRunRequest, RunRecord, RunEvent, RunEventPage, RunFinalAnswer, RunReplayReader, Authorizer, IdentityDirectory, RuntimeStore,
    ModelCatalog, CredentialResolver, DelegationService, IdempotencyConflict, SessionRef, SessionStore)
from .runtime import Runtime, RuntimeSettings, RuntimeServices, deferred_admission_requested
from .provider_history import ProviderHistory, ProviderHistoryMessage
from .provider_admission import ProviderAccount, ProviderRoute, ProviderAdmission
from .deliveries import ConfirmedDelivery
from .capabilities import (CapabilityCatalog, CapabilitySource, ToolExecutor,
    ToolDefinition, ToolDescriptor, FunctionSource, CapabilityUnavailable)
from .extensions import EngineExtensions
from .tool_scope import capability_scope
from .discovery import DISCOVERY_PRESENTATION_API_VERSION, DiscoveryPresentation
from .notifications import NotificationCapability, NotificationRequest, NotificationSink
from .channel_messages import ChannelDestination, ChannelMessageCapability, ChannelMessageRequest, ChannelMessageSink
from .speech_synthesis import SpeechSynthesisCapability
from .modules import (MODULE_API_VERSION, MODULE_SURFACES, CapabilityContribution,
    ModuleCatalog, ModuleConfig, ModuleContext, ModuleContribution, ModuleDescriptor,
    ModuleInstance, ModuleMigration, ModuleReconfigurationError, ModuleReferenceConflict,
    ModuleReferenceInspector, ModuleResolutionError,
    ModuleStatus, ReconfigureReceipt, RuntimeProfile, ServiceBinding, ServiceRegistry)

__version__ = '1.1.0b181'
__all__ = ['Runtime','RuntimeSettings','RuntimeServices','ProviderHistory','ProviderHistoryMessage','ProviderAccount','ProviderRoute','ProviderAdmission','PrincipalRef','ExecutionContext',
    'CapabilityLease','ResourceRef','RunRequest','RunAdmissionReadiness','RunAdmissionPending','WorkerLeaseExpired','deferred_admission_requested','AcceptedRunRequest','RunRecord','RunEvent','RunEventPage','RunFinalAnswer','RunReplayReader','Authorizer','ConfirmedDelivery',
    'AutomationRunKind','AutomationRunCancellationStore',
    'IdentityDirectory','RuntimeStore','SessionRef','SessionStore','ModelCatalog','CredentialResolver','DelegationService',
    'CapabilityCatalog','capability_scope','CapabilitySource','ToolExecutor','ToolDefinition','ToolDescriptor',
    'FunctionSource','CapabilityUnavailable','IdempotencyConflict','EngineExtensions',
    'DISCOVERY_PRESENTATION_API_VERSION','DiscoveryPresentation',
    'NotificationCapability','NotificationRequest','NotificationSink',
    'ChannelDestination','ChannelMessageCapability','ChannelMessageRequest','ChannelMessageSink',
    'SpeechSynthesisCapability',
    'MODULE_API_VERSION','MODULE_SURFACES','CapabilityContribution','ModuleCatalog','ModuleConfig',
    'ModuleContext','ModuleContribution','ModuleDescriptor','ModuleInstance','ModuleMigration',
    'ModuleReconfigurationError','ModuleReferenceConflict','ModuleReferenceInspector',
    'ModuleResolutionError','ModuleStatus','ReconfigureReceipt',
    'RuntimeProfile','ServiceBinding','ServiceRegistry']
