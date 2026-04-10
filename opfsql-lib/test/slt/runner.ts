import type { Engine } from '../../src/index.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type SltBlock =
  | { kind: 'statement'; expectError: boolean; sql: string; skip: boolean }
  | {
      kind: 'query';
      types: string;
      sort: 'nosort' | 'rowsort' | 'valuesort';
      sql: string;
      expected: string[];
      skip: boolean;
    };

export interface SltResult {
  total: number;
  passed: number;
  failed: number;
  skipped: number;
  errors: Array<{ blockIndex: number; sql: string; message: string }>;
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

export function parseSlt(source: string): SltBlock[] {
  const lines = source.split('\n');
  const blocks: SltBlock[] = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i].trim();

    // Skip empty lines and comments
    if (line === '' || line.startsWith('#')) {
      i++;
      continue;
    }

    // Check for skipif
    let skip = false;
    if (line.startsWith('skipif')) {
      const engine = line.split(/\s+/)[1];
      skip = engine === 'opfsql';
      i++;
      // Skip blank lines between skipif and block header
      while (i < lines.length && lines[i].trim() === '') i++;
      if (i >= lines.length) break;
    }

    const header = lines[i].trim();

    // halt — stop parsing
    if (header === 'halt') break;

    if (header.startsWith('statement')) {
      const expectError = header.includes('error');
      i++;

      // Read SQL lines until blank line or EOF
      const sqlLines: string[] = [];
      while (i < lines.length && lines[i].trim() !== '') {
        sqlLines.push(lines[i]);
        i++;
      }

      blocks.push({
        kind: 'statement',
        expectError,
        sql: sqlLines.join('\n').trim(),
        skip,
      });
    } else if (header.startsWith('query')) {
      const parts = header.split(/\s+/);
      const types = parts[1] ?? '';
      const sortStr = parts[2] ?? 'nosort';
      const sort = (['rowsort', 'valuesort'].includes(sortStr) ? sortStr : 'nosort') as
        | 'nosort'
        | 'rowsort'
        | 'valuesort';

      i++;

      // Read SQL lines until ---- or blank line
      const sqlLines: string[] = [];
      while (i < lines.length && lines[i].trim() !== '----' && lines[i].trim() !== '') {
        sqlLines.push(lines[i]);
        i++;
      }

      // Read expected results after ----
      const expected: string[] = [];
      if (i < lines.length && lines[i].trim() === '----') {
        i++; // skip ----
        while (i < lines.length && lines[i].trim() !== '') {
          expected.push(lines[i].trim());
          i++;
        }
      }

      blocks.push({
        kind: 'query',
        types,
        sort,
        sql: sqlLines.join('\n').trim(),
        expected,
        skip,
      });
    } else {
      // Unknown line, skip
      i++;
    }
  }

  return blocks;
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

function formatValue(v: unknown): string {
  if (v === null || v === undefined) return 'NULL';
  return String(v);
}

function formatRow(row: Record<string, unknown>): string {
  return Object.values(row).map(formatValue).join('|');
}

export async function runSlt(engine: Engine, blocks: SltBlock[]): Promise<SltResult> {
  const result: SltResult = { total: 0, passed: 0, failed: 0, skipped: 0, errors: [] };

  for (let idx = 0; idx < blocks.length; idx++) {
    const block = blocks[idx];
    result.total++;

    if (block.skip) {
      result.skipped++;
      continue;
    }

    if (block.kind === 'statement') {
      try {
        await engine.execute(block.sql);
        if (block.expectError) {
          result.failed++;
          result.errors.push({
            blockIndex: idx,
            sql: block.sql,
            message: 'Expected error but statement succeeded',
          });
        } else {
          result.passed++;
        }
      } catch (err) {
        if (block.expectError) {
          result.passed++;
        } else {
          result.failed++;
          result.errors.push({
            blockIndex: idx,
            sql: block.sql,
            message: `Expected success but got error: ${err instanceof Error ? err.message : String(err)}`,
          });
        }
      }
    } else {
      // query
      try {
        const results = await engine.execute(block.sql);
        const queryResult = results[0];

        if (!queryResult || queryResult.type !== 'rows') {
          result.failed++;
          result.errors.push({
            blockIndex: idx,
            sql: block.sql,
            message: `Expected rows result but got ${queryResult?.type ?? 'nothing'}`,
          });
          continue;
        }

        const rows = queryResult.rows ?? [];
        let actual: string[];

        if (block.sort === 'valuesort') {
          // Flatten all values and sort
          const allValues = rows.flatMap((r) => Object.values(r).map(formatValue));
          actual = allValues.sort();
          const expectedSorted = [...block.expected].sort();
          if (actual.join('\n') !== expectedSorted.join('\n')) {
            result.failed++;
            result.errors.push({
              blockIndex: idx,
              sql: block.sql,
              message: `Values mismatch (valuesort)\nExpected:\n  ${expectedSorted.join('\n  ')}\nActual:\n  ${actual.join('\n  ')}`,
            });
            continue;
          }
        } else {
          actual = rows.map(formatRow);

          let expectedCmp = block.expected;
          let actualCmp = actual;

          if (block.sort === 'rowsort') {
            expectedCmp = [...block.expected].sort();
            actualCmp = [...actual].sort();
          }

          if (actualCmp.join('\n') !== expectedCmp.join('\n')) {
            result.failed++;
            result.errors.push({
              blockIndex: idx,
              sql: block.sql,
              message: `Row mismatch (${block.sort})\nExpected:\n  ${expectedCmp.join('\n  ')}\nActual:\n  ${actualCmp.join('\n  ')}`,
            });
            continue;
          }
        }

        result.passed++;
      } catch (err) {
        result.failed++;
        result.errors.push({
          blockIndex: idx,
          sql: block.sql,
          message: `Query threw error: ${err instanceof Error ? err.message : String(err)}`,
        });
      }
    }
  }

  return result;
}
