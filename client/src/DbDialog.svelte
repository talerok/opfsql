<script lang="ts">
  import { onMount } from 'svelte';

  let { currentDb, onopen, onreset }: {
    currentDb: string | null;
    onopen: (name: string) => Promise<void>;
    onreset: () => Promise<void>;
  } = $props();

  let availableDbs: string[] = $state([]);
  let newName = $state('');
  let dialogError: string | null = $state(null);
  let deleteConfirm: string | null = $state(null);

  onMount(scanDbs);

  async function scanDbs() {
    const root = await navigator.storage.getDirectory();
    const names: string[] = [];
    for await (const [name] of (root as any).entries()) {
      if (name.endsWith('.opfsql')) names.push(name.slice(0, -7));
    }
    availableDbs = names.sort();
  }

  async function selectExisting(name: string) {
    dialogError = null;
    try {
      await onopen(name);
    } catch (e: any) {
      dialogError = e.message;
    }
  }

  async function createNew() {
    const name = newName.trim();
    if (!name) return;
    dialogError = null;
    try {
      await onopen(name);
    } catch (e: any) {
      dialogError = e.message;
    }
  }

  async function deleteDb(name: string) {
    dialogError = null;
    try {
      if (currentDb === name) await onreset();
      const root = await navigator.storage.getDirectory();
      try { await root.removeEntry(`${name}.opfsql`); } catch {}
      try { await root.removeEntry(`${name}.opfsql-wal`); } catch {}
      await scanDbs();
    } catch (e: any) {
      dialogError = e.message;
    } finally {
      deleteConfirm = null;
    }
  }

  function handleNewNameKey(e: KeyboardEvent) {
    if (e.key === 'Enter') createNew();
  }
</script>

<div class="overlay">
  <div class="dialog">
    <div class="dialog-title">Open Database</div>

    {#if availableDbs.length > 0}
      <div class="dialog-section-label">Existing</div>
      <div class="db-list">
        {#each availableDbs as db}
          {#if deleteConfirm === db}
            <div class="db-item db-item-confirm">
              <span class="db-item-name">▦ {db}</span>
              <span class="db-confirm-text">Delete permanently?</span>
              <div class="db-confirm-actions">
                <button class="confirm-yes" onclick={() => deleteDb(db)}>Delete</button>
                <button class="confirm-no" onclick={() => deleteConfirm = null}>Cancel</button>
              </div>
            </div>
          {:else}
            <div class="db-item" class:current={db === currentDb}>
              <button class="db-item-open" onclick={() => selectExisting(db)}>
                <span class="db-item-name">▦ {db}</span>
                <span class="db-item-files">{db}.opfsql · {db}.opfsql-wal</span>
              </button>
              <button class="db-delete-btn" onclick={() => deleteConfirm = db} title="Delete">✕</button>
            </div>
          {/if}
        {/each}
      </div>
    {/if}

    <div class="dialog-section-label">New database</div>
    <div class="new-db-row">
      <input
        class="db-input"
        type="text"
        placeholder="database name"
        bind:value={newName}
        onkeydown={handleNewNameKey}
      />
      <button class="dialog-btn" onclick={createNew} disabled={!newName.trim()}>Create</button>
    </div>
    {#if newName.trim()}
      <div class="db-files-preview">
        {newName.trim()}.opfsql · {newName.trim()}.opfsql-wal
      </div>
    {/if}

    {#if dialogError}
      <div class="dialog-error">{dialogError}</div>
    {/if}
  </div>
</div>
