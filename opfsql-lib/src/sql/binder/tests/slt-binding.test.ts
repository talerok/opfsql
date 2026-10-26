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
  const run = new FileRun();
  for (const block of blocksOf(source)) run.block(block);
  return run.failed;
}

/** One file's statements bound in order, each against the schema the DDL before left. */
class FileRun {
  readonly failed: string[] = [];
  private schema = Schema.empty;

  block(block: Block): void {
    for (const statement of parsed(block.sql)) this.bind(statement, block);
  }

  private bind(statement: BindableStatement, block: Block): void {
    try {
      this.schema = applyStatement(
        this.schema,
        bindStatement(statement, this.schema),
      );
    } catch (error) {
      if (!(error instanceof OpfsqlError)) throw error;
      if (!block.expectsError)
        this.failed.push(`${block.sql}: ${error.message}`);
    }
  }
}

/** The bindable statements of a block; none if it does not parse. */
function parsed(sql: string): BindableStatement[] {
  try {
    return bindable(sql);
  } catch (error) {
    if (error instanceof OpfsqlError) return [];
    throw error;
  }
}

describe("the SQL logic tests", () => {
  const files = readdirSync(SLT).filter((file) => file.endsWith(".slt"));

  it.each(files)("%s binds", (file) => {
    expect(failures(readFileSync(new URL(file, SLT), "utf8"))).toEqual([]);
  });
});
