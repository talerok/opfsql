<script lang="ts">
  import type { Schema } from 'opfsql';

  let { schema, onrefresh }: {
    schema: Schema;
    onrefresh: () => void;
  } = $props();

  let expanded = $state(new Set<string>());

  function toggle(name: string) {
    const key = name.toLowerCase();
    if (expanded.has(key)) expanded.delete(key);
    else expanded.add(key);
    expanded = new Set(expanded);
  }
</script>

<aside class="sidebar">
  <div class="sidebar-header">
    <span>Tables</span>
    <button class="refresh-btn" onclick={onrefresh} title="Refresh">↻</button>
  </div>

  {#if schema.tables.length === 0}
    <div class="sidebar-empty">No tables</div>
  {:else}
    {#each schema.tables as table}
      {@const open = expanded.has(table.name.toLowerCase())}
      <div class="tree-table">
        <button class="tree-header" onclick={() => toggle(table.name)}>
          <span class="chevron">{open ? '▾' : '▸'}</span>
          <span class="table-icon">▦</span>
          <span class="tree-name">{table.name}</span>
        </button>

        {#if open}
          <div class="tree-body">
            <div class="tree-section-label">Columns</div>
            {#each table.columns as col}
              <div class="tree-col">
                <span class="col-name">{col.name}</span>
                <span class="type-badge">{col.type}</span>
                {#if col.primaryKey}<span class="badge-pk">PK</span>{/if}
                {#if col.unique && !col.primaryKey}<span class="badge-uq">UQ</span>{/if}
                {#if !col.nullable}<span class="badge-nn">NN</span>{/if}
              </div>
            {/each}

            {#if table.indexes.length > 0}
              <div class="tree-section-label">Indexes</div>
              {#each table.indexes as idx}
                <div class="tree-idx">
                  <span class="idx-name">{idx.name}</span>
                  <span class="idx-exprs">{idx.expressions.join(', ')}</span>
                  {#if idx.unique}<span class="badge-uq">UQ</span>{/if}
                </div>
              {/each}
            {/if}
          </div>
        {/if}
      </div>
    {/each}
  {/if}
</aside>
