export interface Row {
  id: number;
  name: string;
  price: number;
  category: string;
}

export interface BenchmarkRunner {
  name: string;
  storage: string;
  setup(): Promise<void>;
  teardown(): Promise<void>;
  begin(): Promise<void>;
  commit(): Promise<void>;
  insertRow(row: Row): Promise<void>;
  selectAll(): Promise<unknown[]>;
  selectPoint(id: number): Promise<unknown>;
  selectRange(low: number, high: number): Promise<unknown[]>;
  aggregate(): Promise<unknown[]>;
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
