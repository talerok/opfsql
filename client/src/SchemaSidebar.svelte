<script lang="ts">
  import type { CatalogData } from '../../opfsql-lib/src/store/types.js';

  let { schema, onrefresh }: {
    schema: CatalogData;
    onrefresh: () => void;
  } = $props();

  let expanded = $state(new Set<string>());

  function toggle(name: string) {
    const key = name.toLowerCase();
    if (expanded.has(key)) expanded.delete(key);
    else expanded.add(key);
    expanded = new Set(expanded);
  }

  function tableIndexes(tableName: string) {
    return schema.indexes.filter(
      (i) => i.tableName.toLowerCase() === tableName.toLowerCase(),
    );
  }

  function expressionLabel(expr: any): string {
    if (expr.type === 'column') return expr.name;
    if (expr.type === 'json_access') return `${expr.column}->json`;
    return `<${expr.type}>`;
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
      {@const idxs = tableIndexes(table.name)}
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

            {#if idxs.length > 0}
              <div class="tree-section-label">Indexes</div>
              {#each idxs as idx}
                <div class="tree-idx">
                  <span class="idx-name">{idx.name}</span>
                  <span class="idx-exprs">{idx.expressions.map(expressionLabel).join(', ')}</span>
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
