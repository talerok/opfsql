<script lang="ts">
  import { onMount } from 'svelte';
  import { Engine, type Result } from '../../opfsql-lib/src/index.js';
  import { OPFSStorage } from '../../opfsql-lib/src/store/opfs/opfs-storage.js';

  let engine: Engine | null = $state(null);
  let sql = $state('');
  let results: Result[] = $state([]);
  let error: string | null = $state(null);
  let loading = $state(false);
  let status = $state('Initializing engine...');

  onMount(async () => {
    try {
      const backend = new OPFSStorage('sql-client');
      engine = await Engine.create(backend);
      status = 'Ready';
    } catch (e: any) {
      status = 'Failed to initialize';
      error = e.message;
    }
  });

  async function execute() {
    if (!engine || !sql.trim()) return;

    loading = true;
    error = null;

    try {
      results = await engine.execute(sql);
    } catch (e: any) {
      error = e.message;
      results = [];
    } finally {
      loading = false;
    }
  }

  function handleKeydown(e: KeyboardEvent) {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      execute();
    }
  }
</script>

<div class="container">
  <h1>OPFSQL Client</h1>
  <span class="status" class:ready={status === 'Ready'}>{status}</span>

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
                  <td>{val === null ? 'NULL' : val}</td>
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

  <div class="editor">
    <textarea
      bind:value={sql}
      onkeydown={handleKeydown}
      placeholder="Enter SQL query... (Ctrl+Enter to execute)"
      rows="5"
      disabled={!engine}
    ></textarea>
    <button onclick={execute} disabled={!engine || loading || !sql.trim()}>
      {loading ? 'Executing...' : 'Execute'}
    </button>
  </div>
</div>
