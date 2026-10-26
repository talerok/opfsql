<script lang="ts">
  import { onMount } from 'svelte';
  import {
    openDatabase,
    OpfsqlError,
    type Connection,
    type Database,
    type Result,
    type Schema,
    type Value,
  } from 'opfsql';
  import DbDialog from './DbDialog.svelte';
  import SchemaSidebar from './SchemaSidebar.svelte';

  const STORAGE_KEY = 'opfsql-db';

  // The console is one session, so a transaction begun in one run stays open
  // for the next.
  let db: Database | null = null;
  let conn: Connection | null = $state(null);

  let currentDb: string | null = $state(null);
  let status = $state('');
  let showDialog = $state(false);

  let sql = $state('');
  let results: Result[] = $state([]);
  let error: string | null = $state(null);
  let loading = $state(false);
  let schema: Schema = $state({ tables: [] });

  onMount(async () => {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      try {
        await openDb(saved);
      } catch (e) {
        error = describe(e);
        showDialog = true;
      }
    } else {
      showDialog = true;
    }
  });

  async function openDb(name: string) {
    await closeDb();
    status = 'Opening…';
    try {
      const worker = new Worker(new URL('./db-worker.ts', import.meta.url), {
        type: 'module',
      });
      db = await openDatabase(name, { worker });
      conn = await db.connect();
    } catch (e) {
      await closeDb();
      throw e;
    }
    currentDb = name;
    localStorage.setItem(STORAGE_KEY, name);
    status = 'Ready';
    await loadSchema();
  }

  async function closeDb() {
    const closing = db;
    db = null;
    conn = null;
    currentDb = null;
    status = '';
    results = [];
    error = null;
    schema = { tables: [] };
    await closing?.close();
  }

  async function resetDb() {
    await closeDb();
    localStorage.removeItem(STORAGE_KEY);
  }

  async function loadSchema() {
    if (!conn) return;
    try {
      schema = await conn.schema();
    } catch (e) {
      error = describe(e);
    }
  }

  async function execute() {
    if (!sql.trim() || !conn) return;
    loading = true;
    error = null;
    try {
      results = await conn.execute(sql);
    } catch (e) {
      error = describe(e);
      results = [];
    } finally {
      loading = false;
    }
    await loadSchema();
  }

  function describe(e: unknown): string {
    if (e instanceof OpfsqlError) return `${e.code}: ${e.message}`;
    return e instanceof Error ? e.message : String(e);
  }

  function show(value: Value): string {
    if (value === null) return 'NULL';
    if (value instanceof Uint8Array) {
      return `x'${Array.from(value, (b) => b.toString(16).padStart(2, '0')).join('')}'`;
    }
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }

  function handleKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      execute();
    }
  }
</script>

{#if showDialog}
  <DbDialog
    {currentDb}
    onopen={async (name) => { await openDb(name); showDialog = false; }}
    onreset={resetDb}
  />
{/if}

<div class="layout">
  <SchemaSidebar {schema} onrefresh={loadSchema} />

  <main class="main">
    <div class="topbar">
      <h1>OPFSQL</h1>
      {#if currentDb}
        <span class="db-badge">▦ {currentDb}</span>
        <button class="change-btn" onclick={() => showDialog = true}>change</button>
      {/if}
      <span class="status" class:ready={status === 'Ready'}>{status}</span>
    </div>

    <div class="content">
      {#if error}
        <div class="error">{error}</div>
      {/if}

      {#each results as result}
        {#if result.columns.length > 0}
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  {#each result.columns as col}
                    <th>{col}</th>
                  {/each}
                </tr>
              </thead>
              <tbody>
                {#each result.rows as row}
                  <tr>
                    {#each row as val}
                      <td>{show(val)}</td>
                    {/each}
                  </tr>
                {/each}
              </tbody>
            </table>
            <div class="row-count">{result.rows.length} row(s)</div>
          </div>
        {:else}
          <div class="message">OK, {result.changes} row(s) changed</div>
        {/if}
      {/each}
    </div>

    <div class="editor">
      <textarea
        bind:value={sql}
        onkeydown={handleKeydown}
        placeholder="Enter SQL query… (Enter to execute, Shift+Enter for newline)"
        rows="6"
        disabled={status !== 'Ready'}
      ></textarea>
      <div class="actions">
        <button onclick={execute} disabled={status !== 'Ready' || loading || !sql.trim()}>
          {loading ? 'Executing…' : 'Execute'}
        </button>
      </div>
    </div>
  </main>
</div>
