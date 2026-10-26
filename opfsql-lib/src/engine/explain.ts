import {
  formatPhysicalPlan,
  type physical,
  type Statement,
} from "../sql/plan/index.js";

// EXPLAIN: the physical plan a statement runs, as the plan printer writes
// it. A statement that writes shows what it does, then the plan of the rows
// it reads, indented under it; DDL has no plan, only what it does.

const INDENT = "  ";

export function explainStatement(statement: Statement<physical.Plan>): string {
  switch (statement.kind) {
    case "query": {
      return formatPhysicalPlan(statement.plan);
    }
    case "insert": {
      return withSource(
        `insert into ${statement.table.name}`,
        statement.source,
      );
    }
    case "update": {
      return withSource(`update ${statement.table.name}`, statement.source);
    }
    case "delete": {
      return withSource(
        `delete from ${statement.table.name}`,
        statement.source,
      );
    }
    case "create-table": {
      return `create table ${statement.table.name}`;
    }
    case "create-index": {
      return `create index ${statement.index.name} on ${statement.index.table}`;
    }
    case "drop": {
      return `drop ${statement.object} ${statement.name}`;
    }
    case "alter-table": {
      return `alter table ${statement.table.name}`;
    }
  }
}

function withSource(heading: string, source: physical.Plan): string {
  const lines = formatPhysicalPlan(source).split("\n");
  return [heading, ...lines.map((line) => INDENT + line)].join("\n");
}
