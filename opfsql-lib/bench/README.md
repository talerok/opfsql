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
getter calls, but not minified or bundled as the package is either. The
browser runs with a persistent profile in a temporary directory, as a
user's browser does. An ephemeral context is private browsing, where
Chromium's OPFS takes about 100 µs for each read, write or `getSize` of a
sync access handle instead of about 1 µs. There a point query takes about
0.7 ms rather than 0.03 ms.

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

## Browser reference

`baselines/format5-chromium.json` is format 5 in a real browser on real
OPFS. It is one run of
`npm run bench:browser -- --browser chromium --repeats 15 --warmup 5` at
dd02122, in Playwright's headless Chromium 147, with a persistent profile.
The machine was an Apple M4 (10 cores, 16 GB) on macOS 26.4. The run was on
2026-10-26 at 16:57, while other work ran: the 1-minute load average was
12.93 before the run and 16.88 after it.

| Scenario                                        | Median ms | Min ms |   µs/op |
| ----------------------------------------------- | --------: | -----: | ------: |
| insert 1 000 rows in one transaction            |      15.5 |   14.4 |    15.5 |
| insert 100 rows, one commit each                |     114.2 |   97.5 |  1142.0 |
| select by primary key ×1 000                    |      29.1 |   26.1 |    29.1 |
| select range (BETWEEN) ×100                     |     537.7 |  494.6 |  5377.0 |
| full scan ×10                                   |      43.6 |   41.4 |  4360.0 |
| GROUP BY + COUNT + AVG ×10                      |      58.7 |   57.2 |  5870.0 |
| update by primary key, one commit each ×100     |      92.2 |   81.2 |   922.0 |
| delete by primary key, one commit each ×100     |     103.5 |   88.4 |  1035.0 |
| join: an order with its user ×200               |       6.7 |    6.3 |    33.5 |
| join: a user's orders ×20                       |       1.9 |    1.7 |    95.0 |
| join + GROUP BY: order totals per user ×5       |     121.2 |  109.2 | 24240.0 |
| secondary index: account by email ×1 000        |      33.4 |   30.9 |    33.4 |
| keyset pages by primary key, 20 a page          |      20.4 |   17.0 |    40.8 |
| keyset pages by an indexed column, 20 a page    |      37.9 |   34.8 |    75.8 |
| prepared point query, prepared once ×10 000     |     352.4 |  288.9 |    35.2 |
| update all 10 000 rows in one statement         |     125.4 |  116.2 |    12.5 |
| JSON: insert 1 000 documents in one transaction |      25.2 |   18.0 |    25.2 |
| JSON: read back 1 000 documents ×10             |      12.6 |   12.4 |  1260.0 |
| JSON: filter on a path ×10                      |      14.4 |   14.1 |  1440.0 |
| insert then select, one commit each ×100        |     129.3 |  114.2 |  1293.0 |

Compared with the Node bench run minutes later on the same machine:

- A read takes up to about four times its Node time. A point query by
  primary key makes ten file operations: three page reads, three reads and
  two `getSize` calls to pin a snapshot, and two read-mark writes. Each
  takes a few microseconds.
- A scan runs within about 20% of its Node time.
- A commit costs 0.7 to 1 ms more than in Node: it appends to the WAL on
  disk and flushes it.

No scenario is slower than disk I/O explains.

## Before merging

A change to storage, the optimizer or the executor is measured against the
baselines before it merges:

```sh
npm run bench -- --repeats 15 --warmup 5 --compare bench/baselines/format4-node.json
npm run bench -- --repeats 15 --warmup 5 --compare bench/baselines/format5-node.json
```

A scenario that gets slower is explained — by the plan EXPLAIN prints for its
statement — or fixed, before the change merges.
