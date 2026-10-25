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

/**
 * A transaction tried to write after another session committed since it
 * began, so its reads may be stale. Roll it back and run it again.
 */
export class SerializationError extends EngineError {
  constructor() {
    super(
      "could not serialize: the database changed since this transaction began; roll back and retry",
    );
    this.name = "SerializationError";
  }
}

export type ExecuteFn = (params: Value[]) => Promise<Result> | Result;

export class PreparedStatement {
  constructor(private readonly executeFn: ExecuteFn) {}
  async run(params: Value[] = []): Promise<Result> {
    return this.executeFn(params);
  }
}
