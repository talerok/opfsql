import { readdirSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { OpfsqlError } from "../../../errors.js";
import { Schema } from "../../../schema/index.js";
import { parse, StatementType, type Statement } from "../../parser/index.js";
import { bindStatement, type BindableStatement } from "../index.js";
import { applyStatement } from "./catalog.js";

// ---------------------------------------------------------------------------
// Every statement of the SQL logic tests (test/slt) that is expected to
// succeed binds: the binder meets real SQL, statement after statement, its
// schema changed by the DDL before. Expected errors are not checked here —
// most happen when the statement runs.
// ---------------------------------------------------------------------------

const SLT = new URL("../../../../test/slt/", import.meta.url);

interface Block {
  readonly sql: string;
  readonly expectsError: boolean;
}

/** The blocks of an SLT file opfsql runs: `statement …` and `query …`, with their SQL. */
function blocksOf(source: string): Block[] {
  const blocks: Block[] = [];
  const paragraphs = source.split(/\n\s*\n/);
  for (const paragraph of paragraphs) {
    const lines = paragraph
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line !== "" && !line.startsWith("#"));
    const block = blockOf(lines);
    if (block !== null) blocks.push(block);
  }
  return blocks;
}

function blockOf(lines: readonly string[]): Block | null {
  const [first = "", ...rest] = lines;
  const isSkipped = first.startsWith("skipif opfsql");
  const [header = "", ...body] = isSkipped ? rest : lines;
  const isBlock = header.startsWith("statement") || header.startsWith("query");
  if (isSkipped || !isBlock) return null;
  const end = body.indexOf("----");
  const sql = (end === -1 ? body : body.slice(0, end)).join("\n");
  return { sql, expectsError: header === "statement error" };
}

/** The statements of a block the binder takes: EXPLAIN's statement, no transactions. */
function bindable(sql: string): BindableStatement[] {
  return parse(sql).flatMap((statement) => {
    const target = isExplain(statement) ? statement.statement : statement;
    const isTransaction = target.type === StatementType.TRANSACTION_STATEMENT;
    const isNested = target.type === StatementType.EXPLAIN_STATEMENT;
    return isTransaction || isNested ? [] : [target];
  });
}

function isExplain(
  statement: Statement,
): statement is Extract<Statement, { type: StatementType.EXPLAIN_STATEMENT }> {
  return statement.type === StatementType.EXPLAIN_STATEMENT;
}

/** The statements of a file that should bind and do not, with why. */
function failures(source: string): string[] {
  let schema = Schema.empty;
  const failed: string[] = [];
  for (const block of blocksOf(source)) {
    let statements: BindableStatement[];
    try {
      statements = bindable(block.sql);
    } catch {
      continue;
    }
    for (const statement of statements) {
      try {
        schema = applyStatement(schema, bindStatement(statement, schema));
      } catch (error) {
        if (!(error instanceof OpfsqlError)) throw error;
        if (!block.expectsError) failed.push(`${block.sql}: ${error.message}`);
      }
    }
  }
  return failed;
}

describe("the SQL logic tests", () => {
  const files = readdirSync(SLT).filter((file) => file.endsWith(".slt"));

  it.each(files)("%s binds", (file) => {
    expect(failures(readFileSync(new URL(file, SLT), "utf8"))).toEqual([]);
  });
});
