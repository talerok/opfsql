import { OpfsqlError } from "../../src/errors.js";
import { multisetDifference } from "./compare.js";
import { generateRow, literalOf } from "./data.js";
import type { Copies, Copy } from "./database.js";
import { constant, type Expression, print } from "./expression.js";
import { ExpressionGenerator } from "./generate-expression.js";
import { caseSeed, Random } from "./random.js";
import { describeSchema } from "./report.js";
import { RUNTIME_ERRORS } from "./run-check.js";
import type { ColumnSpec, DatabaseSpec, TableSpec } from "./schema.js";
import { AliasSource, tableLeaves } from "./scope.js";

// ---------------------------------------------------------------------------
// Index invariance for statements that change rows: random UPDATE, DELETE
// and INSERT on both copies, which must end the same way and leave the same
// rows, and indexes that agree with them (checkIntegrity).
//
// An UPDATE changes one row at a time, in the order its source finds them,
// so whether a UNIQUE key collides on the way may depend on the plan; and a
// run-time error may too (docs/planning.md, rule 4). When one copy succeeds
// and the other fails, they part, and the changes stop for that database.
// ---------------------------------------------------------------------------

/** Changes made to each database, after its queries. */
const CHANGES = 8;
/** Seeds of changes, apart from those of query cases. */
const FIRST_CHANGE = 100_000;

/** How a change went on one copy. */
type ChangeOutcome =
  | { readonly kind: "done"; readonly changes: number }
  | { readonly kind: "failed"; readonly code: string }
  /** Failed with an error no change should: an internal one, say. */
  | { readonly kind: "broken"; readonly reason: string };

/**
 * How a change went on both copies. They part when it fails at run time on
 * one only, which plans may do — errors are not results, and each copy's
 * plan evaluates its own rows — after which their rows may differ.
 */
type BothOutcome =
  | { readonly kind: "agreed" }
  | { readonly kind: "parted" }
  | { readonly kind: "differ"; readonly difference: string };

export interface ChangesReport {
  readonly failures: readonly string[];
  /** Whether the copies parted: one change succeeded where the other failed. */
  readonly hasParted: boolean;
}

export async function runChanges(
  seed: number,
  spec: DatabaseSpec,
  copies: Copies,
): Promise<ChangesReport> {
  const history: string[] = [];
  const failures: string[] = [];
  let hasParted = false;
  for (let step = 0; step < CHANGES; step++) {
    const random = new Random(caseSeed(seed, FIRST_CHANGE + step));
    const table = random.pick(spec.tables);
    const sql = generateChange(random, spec, table);
    history.push(sql);
    const outcome = await changeBoth(copies, table, sql);
    if (outcome.kind === "agreed") continue;
    hasParted = outcome.kind === "parted";
    if (outcome.kind === "differ") {
      failures.push(describeChanges(seed, spec, history, outcome.difference));
    }
    break;
  }
  const problems = await integrityProblems(copies);
  if (problems !== null) {
    failures.push(describeChanges(seed, spec, history, problems));
  }
  return { failures, hasParted };
}

/** Makes the change on both copies, and says how they compare after it. */
async function changeBoth(
  copies: Copies,
  table: TableSpec,
  sql: string,
): Promise<BothOutcome> {
  const plain = await apply(copies.plain, sql);
  const indexed = await apply(copies.indexed, sql);
  const differ = (difference: string): BothOutcome => ({
    kind: "differ",
    difference,
  });
  if (plain.kind === "broken") return differ(`plain copy: ${plain.reason}`);
  if (indexed.kind === "broken") {
    return differ(`indexed copy: ${indexed.reason}`);
  }
  if (plain.kind !== indexed.kind) return { kind: "parted" };
  const difference = await changeDifference(copies, table, [plain, indexed]);
  return difference === null ? { kind: "agreed" } : differ(difference);
}

function generateChange(
  random: Random,
  spec: DatabaseSpec,
  table: TableSpec,
): string {
  const scope = { leaves: tableLeaves(table, table.name), database: spec };
  const generator = new ExpressionGenerator(random, scope, new AliasSource());
  const where = random.chance(0.85)
    ? ` WHERE ${print(generator.condition(2))}`
    : "";
  switch (random.below(4)) {
    case 0:
    case 1: {
      const columns = random.some(table.columns, random.between(1, 2));
      const assignments = columns.map(
        (column) =>
          `${column.name} = ${print(assigned(random, generator, column))}`,
      );
      return `UPDATE ${table.name} SET ${assignments.join(", ")}${where}`;
    }
    case 2: {
      return `DELETE FROM ${table.name}${where}`;
    }
    default: {
      const rows = Array.from({ length: random.between(1, 3) }, () =>
        generateRow(random, table, random.below(500)),
      );
      const values = rows.map((row) => row.join(", "));
      return `INSERT INTO ${table.name} VALUES (${values.join("), (")})`;
    }
  }
}

/** A new value for `column`, of a type it takes. */
function assigned(
  random: Random,
  generator: ExpressionGenerator,
  column: ColumnSpec,
): Expression {
  switch (column.type) {
    case "INTEGER": {
      return generator.value("integer", 1);
    }
    case "REAL": {
      return generator.value(random.pick(["real", "integer"] as const), 1);
    }
    case "TEXT": {
      return generator.value("text", 1);
    }
    case "BOOLEAN": {
      return generator.condition(1);
    }
    case "ANY": {
      return generator.anyValue(1);
    }
    case "JSON": {
      return constant("untyped", literalOf(random, "JSON"));
    }
  }
}

async function apply(copy: Copy, sql: string): Promise<ChangeOutcome> {
  try {
    const result = await copy.session.query(sql);
    return { kind: "done", changes: result.changes };
  } catch (error) {
    const isExpected =
      error instanceof OpfsqlError &&
      (RUNTIME_ERRORS.has(error.code) || error.code === "CONSTRAINT_VIOLATION");
    if (isExpected) return { kind: "failed", code: error.code };
    const code =
      error instanceof OpfsqlError ? error.code : "not an OpfsqlError";
    return { kind: "broken", reason: `${code}: ${String(error)}` };
  }
}

/** What tells the copies apart after a change both made, or null. */
async function changeDifference(
  copies: Copies,
  table: TableSpec,
  [plain, indexed]: readonly [ChangeOutcome, ChangeOutcome],
): Promise<string | null> {
  if (
    plain.kind === "done" &&
    indexed.kind === "done" &&
    plain.changes !== indexed.changes
  ) {
    return `changed ${String(plain.changes)} rows against ${String(indexed.changes)}`;
  }
  const query = `SELECT * FROM ${table.name}`;
  const plainRows = await copies.plain.session.query(query);
  const indexedRows = await copies.indexed.session.query(query);
  const difference = multisetDifference(plainRows.rows, indexedRows.rows, [
    "the plain copy",
    "the indexed copy",
  ]);
  return difference === null ? null : `${table.name} after it: ${difference}`;
}

async function integrityProblems(copies: Copies): Promise<string | null> {
  const plain = await copies.plain.engine.checkIntegrity();
  const indexed = await copies.indexed.engine.checkIntegrity();
  const problems = [
    ...plain.map((each) => `plain: ${each}`),
    ...indexed.map((each) => `indexed: ${each}`),
  ];
  return problems.length === 0
    ? null
    : `integrity check:\n  ${problems.join("\n  ")}`;
}

function describeChanges(
  seed: number,
  spec: DatabaseSpec,
  history: readonly string[],
  difference: string,
): string {
  return [
    `seed ${String(seed)}, changes failed`,
    difference,
    ...describeSchema(spec),
    "changes, after the queries:",
    ...history.map((sql) => `  ${sql};`),
  ].join("\n");
}
