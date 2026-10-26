import type { SqlType, Value } from "../values/index.js";

// ---------------------------------------------------------------------------
// What a function is: a name, the arguments it takes, the rule for its
// result type (bind time) and its implementation (plan time, then per row).
// The binder and the executor both read these; nothing else knows functions.
// ---------------------------------------------------------------------------

/**
 * The static type of an argument or a result: a SQL type, or NULL for an
 * expression that is always NULL (the NULL literal).
 */
export type StaticType = SqlType | "NULL";

/** Which static types an argument accepts. */
export type ArgumentKind =
  /** Text, numbers and booleans, which have a text form. */
  | "text"
  /** As `text`, plus BLOBs, which the function handles as bytes. */
  | "text-or-blob"
  /** What `||` joins: as `text`, plus JSON, printed as its JSON text. */
  | "concatenable"
  /** INTEGER or REAL. */
  | "number"
  /** JSON, or TEXT holding JSON text. */
  | "json"
  /** A condition: BOOLEAN. */
  | "boolean"
  /** Anything. */
  | "any";

/** The arguments a function takes, in order. */
export interface Signature {
  readonly required: readonly ArgumentKind[];
  readonly optional?: readonly ArgumentKind[];
  /** Any number of further arguments of this kind. */
  readonly rest?: ArgumentKind;
}

/** What the implementation may know about a call before the first row. */
export interface CallSite {
  readonly argumentTypes: readonly StaticType[];
  /** Each argument's value if it is the same for every row, else undefined. */
  readonly constants: readonly (Value | undefined)[];
}

/** An argument of a lazy call: computes its value for a row on demand. */
export type Argument<Row> = (row: Row) => Value;

/**
 * How a call runs per row: `eager` receives the argument values; `lazy`
 * evaluates only the arguments it needs (COALESCE, IIF), so an argument it
 * skips cannot fail.
 */
export type Implementation =
  | {
      readonly kind: "eager";
      readonly call: (values: readonly Value[]) => Value;
    }
  | {
      readonly kind: "lazy";
      readonly call: <Row>(
        operands: readonly Argument<Row>[],
        row: Row,
      ) => Value;
    };

export interface ScalarFunction {
  readonly kind: "scalar";
  /** The name as SQL writes it, lowercase. */
  readonly name: string;
  /**
   * True for a function whose result varies between calls with the same
   * arguments (random): it runs for every row, never folded to a constant.
   */
  readonly isVolatile?: boolean;
  readonly signature: Signature;
  /** Result type for argument types that passed the signature. */
  readonly returnType: (argumentTypes: readonly StaticType[]) => StaticType;
  readonly implement: (call: CallSite) => Implementation;
}

/** Collects the rows of one group and produces the aggregate's value. */
export interface Accumulator {
  /**
   * One row's argument values; rows with a NULL argument never arrive. The
   * array is reused for the next row: keep values, never the array.
   */
  add(values: readonly Value[]): void;
  result(): Value;
}

export interface AggregateFunction {
  readonly kind: "aggregate";
  readonly name: string;
  readonly signature: Signature;
  readonly returnType: (argumentTypes: readonly StaticType[]) => StaticType;
  /** A new, empty accumulator for each group. */
  readonly accumulator: (argumentTypes: readonly StaticType[]) => Accumulator;
}

export type SqlFunction = ScalarFunction | AggregateFunction;
