<script lang="ts">
  import { onMount } from 'svelte';
  import { WorkerEngine, type Connection } from '../../opfsql-lib/src/worker/client.js';
  import type { Result } from '../../opfsql-lib/src/engine/index.js';
  import type { CatalogData } from '../../opfsql-lib/src/store/types.js';
  import DbDialog from './DbDialog.svelte';
  import SchemaSidebar from './SchemaSidebar.svelte';

  const STORAGE_KEY = 'opfsql-db';
  const WORKER_URL = new URL('../../opfsql-lib/src/worker/worker.ts', import.meta.url);

  const engine = new WorkerEngine(WORKER_URL);
  let conn: Connection | null = $state(null);

  let currentDb: string | null = $state(null);
  let status = $state('');
  let showDialog = $state(false);

  let sql = $state('');
  let results: Result[] = $state([]);
  let error: string | null = $state(null);
  let loading = $state(false);
  let schema: CatalogData = $state({ tables: [], indexes: [] });

  onMount(async () => {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      await openDb(saved);
    } else {
      showDialog = true;
    }
  });

  async function openDb(name: string) {
    status = 'Opening…';
    if (conn) { await conn.disconnect(); conn = null; }
    await engine.open(name);
    conn = await engine.connect();
    currentDb = name;
    localStorage.setItem(STORAGE_KEY, name);
    status = 'Ready';
    results = [];
    error = null;
    schema = { tables: [], indexes: [] };
    await loadSchema();
  }

  async function resetDb() {
    if (conn) { await conn.disconnect(); conn = null; }
    await engine.close();
    currentDb = null;
    status = '';
    results = [];
    error = null;
    schema = { tables: [], indexes: [] };
    localStorage.removeItem(STORAGE_KEY);
  }

  async function loadSchema() {
    try { if (conn) schema = await conn.getSchema(); } catch {}
  }

  async function execute() {
    if (!sql.trim() || !conn) return;
    loading = true;
    error = null;
    try {
      results = await conn.exec(sql);
      await loadSchema();
    } catch (e: any) {
      error = e.message;
      results = [];
    } finally {
      loading = false;
    }
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
        {#if result.type === 'rows' && result.rows && result.rows.length > 0}
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  {#each Object.keys(result.rows[0]) as col}
                    <th>{col}</th>
                  {/each}
                </tr>
              </thead>
              <tbody>
                {#each result.rows as row}
                  <tr>
                    {#each Object.values(row) as val}
                      <td>{val === null ? 'NULL' : typeof val === 'object' ? JSON.stringify(val) : val}</td>
                    {/each}
                  </tr>
                {/each}
              </tbody>
            </table>
            <div class="row-count">{result.rows.length} row(s)</div>
          </div>
        {:else if result.type === 'rows'}
          <div class="message">Query returned 0 rows</div>
        {:else if result.type === 'ok'}
          <div class="message">
            OK{result.rowsAffected != null ? `, ${result.rowsAffected} row(s) affected` : ''}
          </div>
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
