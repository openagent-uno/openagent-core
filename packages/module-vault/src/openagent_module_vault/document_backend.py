"""Native agent capabilities for a host-owned document vault."""
from openagent_core.dream_evidence import DocumentDreamEvidence, DreamVaultSource, REVIEW_TOOL
from openagent_core.capabilities import FunctionSource, ToolDefinition, ToolPreconditionFailed
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
    "module.vault.documents", "2",
    """Your long-term memory lives in the authorized document vault. For questions
about the user's preferences, projects or past decisions, call vault_search,
then vault_read_note on relevant hits before answering. Cite the notes you use.
Save durable corrections, preferences, decisions and procedures learned during
work unless the user asks to keep them out of memory. Each note covers one
idea. Include a title, summary and dates in YAML frontmatter and link related
notes using [[folder/name]]. Preserve the requested vault-relative note path exactly,
including its spelling, case and folders. Use paths such as topics/manual.md.
If the user supplies an invalid path, ask for a valid vault-relative path.
Confirm a save only after vault_write_note reports success for that path.
Use vault_write_note with expected_revision=0
for a new note, or the revision you read for an edit. If another writer changed
it, read again and merge the change. Use session history for a transcript.
Keep credentials and transient errors out of the vault.""",
    "openagent-module-vault", section="memory", priority=10,
)


class DocumentRevisionConflict(ToolPreconditionFailed):
    code = "revision_conflict"
    recovery = ("No note was changed. Read the exact requested path. For a new note, create with "
                "expected_revision=0. For an existing note, reconcile its current contents and use "
                "the revision from that read. Confirm only a successful write.")


class DocumentSource(FunctionSource, DreamVaultSource):
    def __init__(self, definitions, functions, vault, evidence):
        super().__init__(definitions, functions)
        self.vault, self.evidence = vault, evidence
        from jsonschema import Draft202012Validator
        self.validators = {tool.name: Draft202012Validator(tool.input_schema) for tool in definitions}

    def validate_tool_arguments(self, name, arguments):
        from openagent_core.memory.vault.documents import note_path
        # Use the storage parser for the byte limit, segments, generated
        # reference and case-insensitive extension. Preserve the user's spelling.
        if "path" in arguments:
            note_path(arguments["path"], allow_generated=name in {"vault_read_note", "vault_history"})
        if "after" in arguments:
            note_path(arguments["after"], allow_generated=True)
        error = next(self.validators[name].iter_errors(arguments), None)
        if error is not None:
            raise ValueError("Invalid document-vault arguments for " + name)


    async def call_tool(self, name, arguments, context):
        from openagent_core.memory.vault.documents import RevisionConflict
        try:
            return await super().call_tool(name, arguments, context)
        except RevisionConflict as error:
            if name != "vault_write_note":
                raise
            # SqlDocumentVault returned through its transaction context manager.
            # This type denotes the failed conditional revision check.
            raise DocumentRevisionConflict() from error

    async def discover(self, context):
        if self.evidence is not None and await self.evidence.enabled(self.vault, context):
            return self.tools
        return tuple(t for t in self.tools if t.name != REVIEW_TOOL)



class DocumentModule:
    domain = "vault"

    def __init__(self, context):
        from openagent_core.memory.vault.documents import SqlDocumentVault
        self.context = context
        self.store = context.services.require(SqlDocumentVault)
        self.contribution = ModuleContribution()
        path = {"type": "string", "pattern": r"^[^/\\]", "minLength": 1,
                "description": "Exact vault-relative note path, such as topics/manual.md. "
                               "Preserve spelling and case. Start with a filename or folder."}
        limit = {"type": "integer", "minimum": 1, "maximum": 200}
        revision = {"type": "integer", "minimum": 0,
                    "description": "Use 0 to create a new note only. To update, read that exact "
                                   "path and supply its current revision."}
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
             {"path": path, "expected_revision": {"type": "integer", "minimum": 1}}, ["path", "expected_revision"], {"vault.write"}),
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
        evidence = context.services.optional(DocumentDreamEvidence)
        if evidence is not None:
            if self.store.dream_queue is None:
                raise ValueError("Dream evidence requires a durable document queue")
            definitions.append(ToolDefinition(REVIEW_TOOL, "Review the recent activity selected for this dream once.", {
                "type": "object", "properties": {}, "required": [], "additionalProperties": False,
            }))
            async def read_evidence(arguments, execution):
                if arguments:
                    raise ValueError("Dream sources and budgets belong to the host")
                return await evidence.read(self.store, execution)
            functions[REVIEW_TOOL] = read_evidence
        self.source = DocumentSource(tuple(definitions), functions, self.store, evidence)

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
