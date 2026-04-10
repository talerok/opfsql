export interface Row {
  id: number;
  name: string;
  price: number;
  category: string;
}

export interface OrderRow {
  id: number;
  product_id: number;
  customer_id: number;
  quantity: number;
  total: number;
}

export interface BenchmarkRunner {
  name: string;
  storage: string;
  setup(): Promise<void>;
  teardown(): Promise<void>;
  insertBatch(rows: Row[]): Promise<void>;
  selectAll(): Promise<unknown[]>;
  selectPoint(id: number): Promise<unknown>;
  selectRange(low: number, high: number): Promise<unknown[]>;
  aggregate(): Promise<unknown[]>;

  // Complex query support (optional — raw-idb doesn't support SQL)
  setupComplex?(productRows: Row[], orderRows: OrderRow[]): Promise<void>;
  teardownComplex?(): Promise<void>;
  joinAgg?(): Promise<unknown[]>;
  joinFilter?(): Promise<unknown[]>;
  subqueryExists?(): Promise<unknown[]>;
  cteJoin?(): Promise<unknown[]>;
  multiAgg?(): Promise<unknown[]>;
}

export interface SuiteDef {
  id: string;
  label: string;
}

export interface BenchResult {
  suiteId: string;
  runner: string;
  ms: number;
}
