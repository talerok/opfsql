# sqllogictest

A chosen part of SQLite's sqllogictest corpus, run against opfsql: SELECT,
joins, aggregates, subqueries, NULL logic and indexes, each query's result
checked against what SQLite gives.

## Where the corpus comes from

The files under `corpus/` are copies of test files from SQLite's
sqllogictest, <https://www.sqlite.org/sqllogictest/>, taken from its GitHub
mirror <https://github.com/gregrahn/sqllogictest> at commit
`c93ac416f05635ff5cc490009add936d9753d01d` (2026-10-07, "Merge the 3.54.0
beta sources for testing"). They keep their paths in that repository, so
`corpus/test/select1.test.gz` is `test/select1.test` there.

The repository's `COPYRIGHT.md` (D. Richard Hipp, 2008) leaves the choice
of terms to the user — GPL, BSD, MIT or CC0 — and asks no attribution. We
take them under CC0, the public-domain dedication.

Each file is stored gzipped (`gzip -9 -n`) and otherwise unchanged; `gunzip
-c corpus/test/select1.test.gz` gives the original. Line numbers anywhere in
this directory are those of the original file. The corpus is never edited:
what opfsql does differently is listed in `skips.txt`.

## What is chosen, and why

| Files                                                                                      | Covers                                                              |
| ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------- |
| `test/select1.test` … `test/select5.test`                                                  | SELECT, subqueries, CASE, joins of up to many tables                |
| `test/evidence/in1.test`, `in2.test`                                                       | IN and NOT IN, with NULLs and empty lists                           |
| `test/evidence/slt_lang_aggfunc.test`                                                      | aggregates, DISTINCT                                                |
| `test/evidence/slt_lang_update.test`, `slt_lang_droptable.test`, `slt_lang_dropindex.test` | UPDATE, DROP TABLE, DROP INDEX                                      |
| `test/index/{between,commute,delete,in,orderby,orderby_nosort,random}/10/slt_good_0.test`  | the same queries over tables with and without indexes: access paths |
| `test/random/{aggregates,expr,groupby,select}/slt_good_0.test`                             | random expressions, groupings and joins                             |

Left out:

- `test/evidence/slt_lang_createtrigger.test`, `slt_lang_droptrigger.test`,
  `slt_lang_createview.test`, `slt_lang_dropview.test`,
  `slt_lang_reindex.test`, `slt_lang_replace.test`: opfsql has no triggers,
  views, REINDEX or REPLACE, so every record of them would be skipped.
- `test/index/view`: views again.
- The larger variants (`100`, `1000`, `10000`) and the other numbered files:
  they repeat the same kinds of queries over more rows, at many times the
  size and the running time.

## Running it

The corpus is part of `npx vitest run`: `select.test.ts`, `select4.test.ts`,
`select5.test.ts`, `evidence.test.ts`, `index.test.ts` and `random.test.ts`
run the files side by side, each file in a fresh database, about 117,000
records in some 20 seconds. `npx vitest run test/sqllogictest` runs them
alone.

`skips.txt` lists each record opfsql is expected to answer differently, by
file and line, with the reason: a departure documented in
docs/semantics.md, or a feature opfsql does not have. A listed record that
starts to pass fails the run too, and `skips.test.ts` checks that every
listed record exists, so the list stays exact. To see what fails and is
not listed, and what is listed and passes:

```sh
npx vite-node test/sqllogictest/triage.ts [corpus path …]
```

## Updating the corpus

Clone the mirror into a directory of its own, without running anything
from it, and check out only the chosen files:

```sh
git clone --depth 1 --filter=blob:none --no-checkout \
  https://github.com/gregrahn/sqllogictest.git
cd sqllogictest
git sparse-checkout set --no-cone /test/select1.test /test/evidence/in1.test …
git checkout
```

Then gzip each file into place, `gzip -9 -n -c <path> > corpus/<path>.gz`,
record the new commit above, and bring `skips.txt` up to date with the
triage script: line numbers move when the files change.
