import type { Row, Value } from "../types.js";

export type Result =
  | { type: "rows"; rows: Row[]; rowsAffected?: undefined }
  | { type: "ok"; rowsAffected: number; rows?: undefined };

export class EngineError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "EngineError";
  }
}

export type ExecuteFn = (params: Value[]) => Promise<Result> | Result;

export class PreparedStatement {
  constructor(private readonly executeFn: ExecuteFn) {}
  async run(params: Value[] = []): Promise<Result> {
    return this.executeFn(params);
  }
}
