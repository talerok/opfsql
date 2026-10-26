# Benchmarks

Workloads run through the public SQL API (`scenarios.ts`), so the same file
measures any revision of the engine.

## Running

```sh
npm run bench                                   # Node: the bundled engine, memory files
npm run bench:browser -- --browser chromium     # a browser worker: real OPFS
npm run bench -- --filter join                  # only scenarios whose name has "join"
npm run bench -- --repeats 15 --warmup 5 --json results.json
npm run bench -- --compare bench/baselines/format4-node.json
npm run bench:profile -- --scenario "full scan" # where one scenario's CPU time goes
```

Each scenario reports the median of `--repeats` timed runs after `--warmup`
untimed ones, and microseconds per operation. A read-only scenario prepares
its database once; any other gets a fresh one per run.

## What `npm run bench` measures

The engine as users run it: `npm run bench` bundles `node.ts` and everything
it imports with esbuild — ESM, minified like the package build, with a
sourcemap for profiles — into `bench/dist`, and runs the bundle with
`node --expose-gc`. Running the sources through vite-node instead would
measure code no user runs: its transform turns every imported function into
a getter call on a module object, which keeps V8 from inlining hot helpers.
`npm run bench:profile` profiles the same build: `profile.ts` is the
bundle's second entry, and it reads each minified frame back through the
sourcemap to its function's own name and source file.

Files live in memory (opfs-mock), so the Node bench measures the CPU path:
planning, executing, encoding and B-tree work, not disk I/O.
`npm run bench:browser` adds the browser's real OPFS. It serves the modules
unbundled from vite's dev server: native ES modules, so imports are not
getter calls, but not minified or bundled as the package is either.

## Baselines

| File                          | Engine                                          |
| ----------------------------- | ----------------------------------------------- |
| `baselines/format4-node.json` | the format-4 engine at 93fa284, its last commit |
| `baselines/format5-node.json` | format 5 and the new pipeline at 5b38901        |

Both are bundled Node runs of every scenario in `scenarios.ts`,
`--repeats 15 --warmup 5`, on one machine (10 cores) on 2026-10-26, each
in the quietest window that day offered. The 1-minute load average
(`uptime`) before and after each run:

- format 4: 3.30, then 3.52 (14:35)
- format 5: 3.36, then 4.63 (15:23)

The format-4 engine runs every scenario; it takes only a constant LIMIT,
so the keyset pages write `LIMIT 20` rather than a parameter.

Timings move with the machine, so compare runs made on one machine, close in
time; a baseline says which way a change goes and roughly how far.

## Before merging

A change to storage, the optimizer or the executor is measured against the
baselines before it merges:

```sh
npm run bench -- --repeats 15 --warmup 5 --compare bench/baselines/format4-node.json
npm run bench -- --repeats 15 --warmup 5 --compare bench/baselines/format5-node.json
```

A scenario that gets slower is explained — by the plan EXPLAIN prints for its
statement — or fixed, before the change merges.
