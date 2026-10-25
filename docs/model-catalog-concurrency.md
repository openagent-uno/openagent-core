# Catalog writes alongside runtime persistence

Model-manager keeps a shared read connection. A still-open catalog reader can retain a WAL snapshot while another runtime connection commits. Promoting that snapshot into a writer raises SQLITE_BUSY_SNAPSHOT immediately; increasing busy_timeout or replaying the same tool call cannot repair it.

Provider/model upserts, enable/default flags and removals now use short dedicated BEGIN IMMEDIATE transactions with the configured SQLite lock timeout. Validation, mutation and ID lookup share that transaction, which commits or rolls back before closing. Existing atomic model/provider edits retain their dedicated transactions. Other MemoryDB writers are unchanged. Concurrent first MCP calls wait until the singleton database has finished initialization; failed initialization is closed and never published.

Regression tests retain a live reader, advance the WAL on an independent connection, then exercise additions and catalog mutations. Parallel MCP additions and a rejected provider reference verify subsequent writes remain usable. Initial MCP connection publication is tested separately.
