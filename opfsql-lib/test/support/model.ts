import { pick, type Random } from "./random.js";

/** A row of `t (id INTEGER PRIMARY KEY, v INTEGER)`, as the tests read it. */
export interface Row {
  readonly id: number;
  readonly v: number;
}

/** What table t should hold: v by id, and the next id to insert. */
export class Model {
  private readonly values = new Map<number, number>();
  private nextId = 1;

  get isEmpty(): boolean {
    return this.values.size === 0;
  }

  /** The rows as `SELECT id, v FROM t ORDER BY id` returns them. */
  rows(): Row[] {
    return [...this.values]
      .toSorted(([a], [b]) => a - b)
      .map(([id, v]) => ({ id, v }));
  }

  newId(): number {
    return this.nextId++;
  }

  anyId(random: Random): number {
    return pick(random, [...this.values.keys()]);
  }

  set(id: number, v: number): void {
    this.values.set(id, v);
  }

  delete(id: number): void {
    this.values.delete(id);
  }
}
