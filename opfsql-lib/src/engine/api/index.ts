// The engine's public contract: plain types and the Result class, depending
// on nothing but values and errors, so the worker client can use them
// without loading the engine.

export type {
  CallOptions,
  EngineContract,
  EngineOpener,
  OpenOptions,
  SessionContract,
  SessionOptions,
  StatementContract,
  TransactionMode,
  TransactionOptions,
} from "./contract.js";
export {
  emptyResult,
  Result,
  type ResultData,
  type ResultRow,
} from "./result.js";
export type { ColumnInfo, IndexInfo, Schema, TableInfo } from "./schema.js";
