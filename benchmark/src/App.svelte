<script lang="ts">
  import { SUITES, runSuite } from './suites.js';
  import type { BenchmarkRunner } from './types.js';
  import { createOpfsqlRunner } from './runners/opfsql.js';
  import { createAlasqlRunner } from './runners/alasql.js';
  import { createWaSqliteRunner } from './runners/wa-sqlite.js';
  import { createRawIdbRunner } from './runners/raw-idb.js';
  import { createOpfsqlMemoryRunner } from './runners/opfsql-memory.js';

  const runners: BenchmarkRunner[] = [
    createOpfsqlRunner(),
    createOpfsqlMemoryRunner(),
    createAlasqlRunner(),
    // createWaSqliteRunner(),
    createRawIdbRunner(),
  ];

  // results[suiteId][runnerName] = ms | -1 (running) | NaN (error)
  let results = $state<Record<string, Record<string, number>>>({});
  let running = $state(false);
  let status = $state('');

  function bestTime(suiteId: string): number {
    const sr = results[suiteId];
    if (!sr) return -1;
    const times = Object.values(sr).filter((v) => v > 0 && !isNaN(v));
    return times.length > 0 ? Math.min(...times) : -1;
  }

  function cellText(ms: number | undefined): string {
    if (ms === undefined) return '\u2014';
    if (ms < 0) return 'running...';
    if (isNaN(ms)) return 'error';
    return `${ms.toFixed(1)} ms`;
  }

  async function runAll() {
    running = true;
    results = {};

    for (const suite of SUITES) {
      for (const runner of runners) {
        status = `${suite.label} \u2192 ${runner.name}...`;
        results[suite.id] = { ...results[suite.id], [runner.name]: -1 };

        try {
          const ms = await runSuite(suite.id, runner);
          results[suite.id] = { ...results[suite.id], [runner.name]: ms };
        } catch (err) {
          console.error(`${suite.id} / ${runner.name} failed:`, err);
          results[suite.id] = { ...results[suite.id], [runner.name]: NaN };
        }
      }
    }

    status = 'Done!';
    running = false;
  }
</script>

<div class="bench">
  <h1>opfsql Benchmark</h1>
  <p class="subtitle">opfsql (OPFS / In-memory) vs alasql (memory) vs wa-sqlite (IndexedDB) vs Raw IndexedDB</p>

  <button onclick={runAll} disabled={running}>Run Benchmark</button>
  <div class="status">{status}</div>

  <table>
    <thead>
      <tr>
        <th>Suite</th>
        {#each runners as r}
          <th class="runner-header">{r.name}<br><span class="storage">{r.storage}</span></th>
        {/each}
      </tr>
    </thead>
    <tbody>
      {#each SUITES as suite}
        {@const best = bestTime(suite.id)}
        <tr>
          <td>{suite.label}</td>
          {#each runners as r}
            {@const ms = results[suite.id]?.[r.name]}
            <td class:best={ms !== undefined && ms > 0 && ms === best} class:running={ms !== undefined && ms < 0}>
              {cellText(ms)}
            </td>
          {/each}
        </tr>
      {/each}
    </tbody>
  </table>
</div>

<style>
  :global(body) {
    font-family: system-ui, -apple-system, sans-serif;
    padding: 2rem;
    background: #0a0a0a;
    color: #e0e0e0;
    margin: 0;
  }
  h1 { font-size: 1.5rem; margin-bottom: 0.5rem; }
  .subtitle { color: #888; margin-bottom: 1.5rem; font-size: 0.9rem; }
  button {
    padding: 0.6rem 1.5rem;
    font-size: 1rem;
    cursor: pointer;
    background: #2563eb;
    color: white;
    border: none;
    border-radius: 6px;
    margin-bottom: 1.5rem;
  }
  button:hover { background: #1d4ed8; }
  button:disabled { opacity: 0.5; cursor: not-allowed; }
  .status { margin-bottom: 1rem; color: #aaa; min-height: 1.4em; }
  table { border-collapse: collapse; width: 100%; max-width: 900px; }
  th, td { padding: 0.5rem 1rem; text-align: right; border: 1px solid #333; }
  th { background: #1a1a1a; font-weight: 600; }
  th:first-child, td:first-child { text-align: left; }
  .best { color: #22c55e; font-weight: 700; }
  .running { color: #facc15; }
  .runner-header { font-size: 0.85rem; }
  .storage { color: #888; font-weight: 400; }
</style>
