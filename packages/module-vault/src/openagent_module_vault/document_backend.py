"""Native agent capabilities for a host-owned document vault."""
from openagent_core.capabilities import FunctionSource, ToolDefinition
from openagent_core.module_adapters import NativeCapabilityDescriptor
from openagent_core.modules import CapabilityContribution, ModuleContribution, ServiceBinding
from openagent_core.prompts import PromptBlock, rule_blocks


class VaultDescriptor(NativeCapabilityDescriptor):
    def validate(self, config):
        backend = config.options.get("backend", "files")
        if backend == "files":
            return super().validate(config)
        if backend != "documents":
            raise ValueError("Unknown vault backend")
        if "service" not in config.surfaces or config.surfaces - {"service", "agent_tools", "host_api", "workers"}:
            raise ValueError("Invalid document vault surfaces")
        if "workers" in config.surfaces and "agent_tools" not in config.surfaces:
            raise ValueError("Document dream workers require native vault tools")
        if set(config.options) - {"backend"}:
            raise ValueError("Document vault storage and policy are supplied by the host service")

    async def prepare(self, context):
        if context.config.options.get("backend") == "documents":
            return DocumentModule(context)
        return await super().prepare(context)


_DOCUMENT_PROMPT = PromptBlock(
    "module.vault.documents", "1",
    """Your long-term memory lives in the authorized document vault. For questions
about the user's preferences, projects or past decisions, call vault_search,
then vault_read_note on relevant hits before answering. Cite the notes you use.
Save durable corrections, preferences, decisions and procedures learned during
work unless the user asks to keep them out of memory. Each note covers one
idea. Include a title, summary and dates in YAML frontmatter and link related
notes using [[folder/name]]. Use vault_write_note with expected_revision=0
for a new note, or the revision you read for an edit. If another writer changed
it, read again and merge the change. Use session history for a transcript.
Keep credentials and transient errors out of the vault.""",
    "openagent-module-vault", section="memory", priority=10,
)


class DocumentModule:
    domain = "vault"

    def __init__(self, context):
        from openagent_core.memory.vault.documents import SqlDocumentVault
        self.context = context
        self.store = context.services.require(SqlDocumentVault)
        self.contribution = ModuleContribution()
        path = {"type": "string", "description": "Vault-relative Markdown note path"}
        limit = {"type": "integer", "minimum": 1, "maximum": 200}
        revision = {"type": "integer", "minimum": 0}
        actions = (
            ("vault_read_note", self.store.read, "Read the Markdown and current revision of a note.",
             {"path": path, "revision": {"type": "integer", "minimum": 1}}, ["path"], {"vault.read", "vault.recall.path"}),
            ("vault_write_note", self.store.write, "Save Markdown when its current revision matches.",
             {"path": path, "content": {"type": "string"}, "expected_revision": revision},
             ["path", "content", "expected_revision"], {"vault.write"}),
            ("vault_search", self.store.search, "Search the authorized memory space by words.",
             {"query": {"type": "string"}, "limit": limit}, ["query"], {"vault.search"}),
            ("vault_list_notes", self.store.list_notes, "List notes in path order.",
             {"after": path, "limit": limit}, [], {"vault.inspect"}),
            ("vault_backlinks", self.store.backlinks, "List notes with a link to this full path.",
             {"path": path, "after": path, "limit": limit}, ["path"], {"vault.inspect"}),
            ("vault_history", self.store.history, "List saved revisions of a note.",
             {"path": path, "before": revision, "limit": limit}, ["path"], {"vault.inspect"}),
            ("vault_forget_note", self.store.forget, "Forget the note and all saved revisions.",
             {"path": path, "expected_revision": revision}, ["path", "expected_revision"], {"vault.write"}),
        )
        definitions = []
        functions = {}
        self.effects = {}
        for name, operation, description, properties, required, effects in actions:
            definitions.append(ToolDefinition(name, description, {
                "type": "object", "properties": properties, "required": required,
                "additionalProperties": False,
            }))
            functions[name] = self._handler(operation)
            self.effects[name] = frozenset(effects)
        self.source = FunctionSource(tuple(definitions), functions)

    @staticmethod
    def _handler(operation):
        async def invoke(arguments, context):
            return await operation(context, **arguments)
        return invoke

    async def start(self):
        tools = "agent_tools" in self.context.config.surfaces
        workers = ()
        if "workers" in self.context.config.surfaces:
            from openagent_core.memory.vault.document_dream import DocumentDreamWorker
            worker = self.context.services.require(DocumentDreamWorker)
            if worker.queue is not self.store.dream_queue:
                raise ValueError("The dream worker must use this vault's queue")
            workers = (worker,)
        self.contribution = ModuleContribution(
            workers=workers,
            services=(ServiceBinding("vault.service", self),),
            capabilities=(CapabilityContribution(
                "vault", self.source, self.source, "Authorized memory", trusted_effects=self.effects
            ),) if tools else (),
            prompt_blocks=(_DOCUMENT_PROMPT, *rule_blocks("module.vault.checklist")) if tools else (),
            search_providers=(self,),
        )
        return self.contribution

    async def inspect_tools(self, context):
        return await self.source.discover(context)

    async def call(self, source_name, tool_name, arguments, context):
        if source_name != self.domain:
            raise LookupError("Unknown document vault source")
        return await self.source.call_tool(tool_name, arguments, context)

    async def search(self, context, *, query, limit=20):
        return await self.store.search(context, query=query, limit=limit)

    async def recall_documents(self, context, *, query, limit):
        result = await self.store.search(context, query=query, limit=limit)
        return result["hits"]

    async def reconfigure(self, config):
        if config != self.context.config:
            raise RuntimeError("Vault configuration changes use an atomic graph replacement")
        return self.contribution

    async def drain(self):
        pass

    async def close(self):
        pass
