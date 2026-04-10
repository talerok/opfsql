import type { BenchmarkRunner, Row, SuiteDef } from './types.js';

const CATEGORIES = ['Electronics', 'Books', 'Clothing', 'Food', 'Toys'];

export function generateRows(n: number): Row[] {
  const rows: Row[] = [];
  for (let i = 0; i < n; i++) {
    rows.push({
      id: i,
      name: `Product ${i}`,
      price: Math.round(Math.random() * 10000) / 100,
      category: CATEGORIES[i % CATEGORIES.length],
    });
  }
  return rows;
}

async function insertN(runner: BenchmarkRunner, rows: Row[]) {
  await runner.begin();
  for (const row of rows) {
    await runner.insertRow(row);
  }
  await runner.commit();
}

async function prepareData(runner: BenchmarkRunner, n: number) {
  await runner.setup();
  const rows = generateRows(n);
  await insertN(runner, rows);
  return rows;
}

export const SUITES: SuiteDef[] = [
  { id: 'insert-1k', label: 'INSERT 1 000 rows' },
  { id: 'insert-10k', label: 'INSERT 10 000 rows' },
  { id: 'select-all', label: 'SELECT * (10k rows)' },
  { id: 'select-point', label: 'SELECT by PK (×100)' },
  { id: 'select-range', label: 'SELECT range (BETWEEN)' },
  { id: 'aggregate', label: 'GROUP BY + COUNT + AVG' },
];

export async function runSuite(
  suiteId: string,
  runner: BenchmarkRunner,
): Promise<number> {
  const t0 = performance.now();

  switch (suiteId) {
    case 'insert-1k': {
      await runner.setup();
      const rows = generateRows(1000);
      const start = performance.now();
      await insertN(runner, rows);
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    case 'insert-10k': {
      await runner.setup();
      const rows = generateRows(10000);
      const start = performance.now();
      await insertN(runner, rows);
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    case 'select-all': {
      await prepareData(runner, 10000);
      const start = performance.now();
      await runner.selectAll();
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    case 'select-point': {
      await prepareData(runner, 10000);
      const ids = Array.from({ length: 100 }, () =>
        Math.floor(Math.random() * 10000),
      );
      const start = performance.now();
      for (const id of ids) {
        await runner.selectPoint(id);
      }
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    case 'select-range': {
      await prepareData(runner, 10000);
      const start = performance.now();
      await runner.selectRange(25, 75);
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    case 'aggregate': {
      await prepareData(runner, 10000);
      const start = performance.now();
      await runner.aggregate();
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    default:
      throw new Error(`Unknown suite: ${suiteId}`);
  }
}
