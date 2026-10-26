import { NotSupportedError, SemanticError } from "../../errors.js";
import {
  resolveCall,
  sqlFunction,
  type AggregateFunction,
} from "../../functions/index.js";
import { formatExpression, type FunctionExpression } from "../parser/index.js";
import type { logical } from "../plan/index.js";
import { refuse, type Clause } from "./clause.js";
import { columnsReadBy } from "./walk.js";

// ---------------------------------------------------------------------------
// Function calls, resolved and typed through the function registry: an
// unknown name, a wrong number of arguments or an argument of a type the
// function does not take is a SemanticError here (docs/semantics.md,
// Functions). An aggregate call goes to the clause's collector, which puts
// it in the query's aggregate node; the expression reads its column.
// ---------------------------------------------------------------------------

type Expression = logical.Expression;

export function bindCall(call: FunctionExpression, clause: Clause): Expression {
  const name = call.functionName;
  const definition = sqlFunction(name);
  if (definition.kind === "aggregate") {
    return bindAggregate(call, definition, clause);
  }
  if (call.isDistinct || call.isStar) {
    throw new SemanticError(`${name}() is not an aggregate function`);
  }
  const argumentList = call.children.map((child) => clause.bind(child));
  const types = argumentList.map((argument) => argument.type);
  const type = resolveCall(definition, types);
  return {
    kind: "function",
    name,
    function: definition,
    arguments: argumentList,
    type,
  };
}

function bindAggregate(
  call: FunctionExpression,
  definition: AggregateFunction,
  clause: Clause,
): Expression {
  const rule = clause.aggregates;
  if (rule.kind === "refuse") {
    throw new SemanticError(
      `aggregate functions are not allowed in ${rule.where}`,
    );
  }
  const inner = clause.withAggregates(
    refuse("the arguments of an aggregate function"),
  );
  const argumentList = call.children.map((child) => inner.bind(child));
  // As in SQLite: DISTINCT compares one value, group_concat's and no other.
  if (call.isDistinct && argumentList.length !== 1) {
    throw new SemanticError(
      `${call.functionName}(DISTINCT …) takes exactly one argument`,
    );
  }
  checkLevel(argumentList);
  const types = argumentList.map((argument) => argument.type);
  const type = resolveCall(definition, types);
  const bound = {
    name: call.functionName,
    function: definition,
    arguments: argumentList,
    isDistinct: call.isDistinct,
    type,
  };
  return rule.collector.add(bound, formatExpression(call));
}

/**
 * An aggregate whose arguments read only an enclosing query's columns
 * belongs to that query in standard SQL; opfsql aggregates where it is
 * written, so it refuses what it would compute differently.
 */
function checkLevel(argumentList: readonly Expression[]): void {
  const columns = argumentList.flatMap((argument) => [
    ...columnsReadBy(argument),
  ]);
  const isOuterOnly =
    columns.length > 0 && columns.every((column) => column.depth > 0);
  if (isOuterOnly) {
    throw new NotSupportedError(
      "an aggregate over only an enclosing query's columns",
    );
  }
}
