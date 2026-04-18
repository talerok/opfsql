import type { Row, Value } from "../types.js";

export interface Result {
  type: "rows" | "ok";
  rows?: Row[];
  rowsAffected?: number;
}

export class EngineError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EngineError";
  }
}

export type ExecuteFn = (params: Value[]) => Result;

export class PreparedStatement {
  constructor(private readonly executeFn: ExecuteFn) {}
  run(params: Value[] = []): Result {
    return this.executeFn(params);
  }
}
