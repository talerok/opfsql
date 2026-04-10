import type { BenchmarkRunner, OrderRow, Row, SuiteDef } from "./types.js";

const CATEGORIES = ["Electronics", "Books", "Clothing", "Food", "Toys"];

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

export function generateOrders(n: number, productCount: number): OrderRow[] {
  const orders: OrderRow[] = [];
  for (let i = 0; i < n; i++) {
    const productId = Math.floor(Math.random() * productCount);
    const qty = 1 + Math.floor(Math.random() * 20);
    orders.push({
      id: i,
      product_id: productId,
      customer_id: Math.floor(Math.random() * 500),
      quantity: qty,
      total: Math.round(qty * (Math.random() * 100) * 100) / 100,
    });
  }
  return orders;
}

async function prepareData(runner: BenchmarkRunner, n: number) {
  await runner.setup();
  const rows = generateRows(n);
  await runner.insertBatch(rows);
  return rows;
}

async function prepareComplex(runner: BenchmarkRunner) {
  const products = generateRows(5000);
  const orders = generateOrders(5000, 5000);
  await runner.setupComplex!(products, orders);
}

export const SUITES: SuiteDef[] = [
  { id: "insert-1k", label: "INSERT 1 000 rows" },
  { id: "insert-10k", label: "INSERT 10 000 rows" },
  { id: "select-all", label: "SELECT * (10k rows)" },
  { id: "select-point", label: "SELECT by PK (×100)" },
  { id: "select-range", label: "SELECT range (BETWEEN)" },
  { id: "aggregate", label: "GROUP BY + COUNT + AVG" },
  { id: "join-agg", label: "JOIN + GROUP BY (5k×5k)" },
  { id: "join-filter", label: "JOIN + filter pushdown (5k×5k)" },
  { id: "subquery-exists", label: "Subquery EXISTS (5k×5k)" },
  { id: "cte-join", label: "CTE + JOIN + ORDER (5k×5k)" },
  { id: "multi-agg", label: "JOIN + COUNT DISTINCT (5k×5k)" },
];

export async function runSuite(
  suiteId: string,
  runner: BenchmarkRunner,
): Promise<number> {
  switch (suiteId) {
    case "insert-1k": {
      await runner.setup();
      const rows = generateRows(1000);
      const start = performance.now();
      await runner.insertBatch(rows);
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    case "insert-10k": {
      await runner.setup();
      const rows = generateRows(10000);
      const start = performance.now();
      await runner.insertBatch(rows);
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    case "select-all": {
      await prepareData(runner, 10000);
      const start = performance.now();
      await runner.selectAll();
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    case "select-point": {
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

    case "select-range": {
      await prepareData(runner, 10000);
      const start = performance.now();
      await runner.selectRange(25, 75);
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    case "aggregate": {
      await prepareData(runner, 10000);
      const start = performance.now();
      await runner.aggregate();
      const ms = performance.now() - start;
      await runner.teardown();
      return ms;
    }

    // --- Complex query suites ---

    case "join-agg": {
      if (!runner.setupComplex) return -2;
      await prepareComplex(runner);
      const start = performance.now();
      await runner.joinAgg!();
      const ms = performance.now() - start;
      await runner.teardownComplex!();
      return ms;
    }

    case "join-filter": {
      if (!runner.setupComplex) return -2;
      await prepareComplex(runner);
      const start = performance.now();
      await runner.joinFilter!();
      const ms = performance.now() - start;
      await runner.teardownComplex!();
      return ms;
    }

    case "subquery-exists": {
      if (!runner.setupComplex) return -2;
      await prepareComplex(runner);
      const start = performance.now();
      await runner.subqueryExists!();
      const ms = performance.now() - start;
      await runner.teardownComplex!();
      return ms;
    }

    case "cte-join": {
      if (!runner.setupComplex) return -2;
      await prepareComplex(runner);
      const start = performance.now();
      await runner.cteJoin!();
      const ms = performance.now() - start;
      await runner.teardownComplex!();
      return ms;
    }

    case "multi-agg": {
      if (!runner.setupComplex) return -2;
      await prepareComplex(runner);
      const start = performance.now();
      await runner.multiAgg!();
      const ms = performance.now() - start;
      await runner.teardownComplex!();
      return ms;
    }

    default:
      throw new Error(`Unknown suite: ${suiteId}`);
  }
}
