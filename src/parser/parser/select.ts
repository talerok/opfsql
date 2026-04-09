import { BaseParser } from './base.js';
import { parseExpression } from './expressions.js';
import {
  TokenType, ParsedExpression,
  TableRefType, JoinType, TableRef, BaseTableRef, JoinRef, SubqueryRef,
  ResultModifierType, OrderType, OrderByNullType,
  OrderByNode, OrderModifier, LimitModifier, DistinctModifier, ResultModifier,
  GroupByNode, CTEMap, SelectNode, SetOperationNode, SetOperationType,
  StatementType, SelectStatement, SetOperationStatement,
} from '../types.js';

export function parseSelectStatement(p: BaseParser): SelectStatement | SetOperationStatement {
  const cte_map = parseCTE(p);
  const node = parseSelectNode(p, cte_map);

  if (p.check(TokenType.UNION)) {
    return parseSetOperation(p, node, cte_map);
  }

  parseTrailingModifiers(p, node);

  return { type: StatementType.SELECT_STATEMENT, node };
}

function parseSelectNode(p: BaseParser, cte_map: CTEMap): SelectNode {
  p.expect(TokenType.SELECT, `Expected 'SELECT'`);

  const modifiers: ResultModifier[] = [];

  if (p.match(TokenType.DISTINCT)) {
    modifiers.push({
      type: ResultModifierType.DISTINCT_MODIFIER,
      distinct_on_targets: [],
    } satisfies DistinctModifier);
  }

  const select_list = parseSelectList(p);

  let from_table: TableRef | null = null;
  if (p.match(TokenType.FROM)) {
    from_table = parseFrom(p);
  }

  let where_clause: ParsedExpression | null = null;
  if (p.match(TokenType.WHERE)) {
    where_clause = parseExpression(p);
  }

  const groups = parseGroupBy(p);

  let having: ParsedExpression | null = null;
  if (p.match(TokenType.HAVING)) {
    having = parseExpression(p);
  }

  return {
    type: 'SELECT_NODE',
    select_list,
    from_table,
    where_clause,
    groups,
    having,
    modifiers,
    cte_map,
  };
}

function parseTrailingModifiers(p: BaseParser, node: SelectNode): void {
  if (p.check(TokenType.ORDER)) {
    const orders = parseOrderBy(p);
    node.modifiers.push({
      type: ResultModifierType.ORDER_MODIFIER,
      orders,
    } satisfies OrderModifier);
  }

  if (p.check(TokenType.LIMIT) || p.check(TokenType.OFFSET)) {
    node.modifiers.push(parseLimitOffset(p));
  }
}

function parseSetOperation(p: BaseParser, left: SelectNode, cte_map: CTEMap): SetOperationStatement {
  let currentLeft: SelectNode | SetOperationNode = left;

  while (p.check(TokenType.UNION)) {
    p.advance();
    const isAll = p.match(TokenType.ALL);
    const right = parseSelectNode(p, { map: {} });

    currentLeft = {
      type: 'SET_OPERATION_NODE',
      set_op_type: isAll ? SetOperationType.UNION_ALL : SetOperationType.UNION,
      left: currentLeft,
      right,
      modifiers: [],
      cte_map,
    };
  }

  const setNode = currentLeft as SetOperationNode;

  if (p.check(TokenType.ORDER)) {
    const orders = parseOrderBy(p);
    setNode.modifiers.push({ type: ResultModifierType.ORDER_MODIFIER, orders });
  }

  if (p.check(TokenType.LIMIT) || p.check(TokenType.OFFSET)) {
    setNode.modifiers.push(parseLimitOffset(p));
  }

  return { type: StatementType.SELECT_STATEMENT, node: setNode };
}

// ===========================================================================
// Select list
// ===========================================================================

function parseSelectList(p: BaseParser): ParsedExpression[] {
  const list: ParsedExpression[] = [];

  if (p.isClauseKeyword()) {
    p.error(`Expected expression after SELECT, got '${p.peek().value || 'EOF'}'`);
  }

  list.push(parseSelectExpression(p));
  while (p.match(TokenType.COMMA)) {
    list.push(parseSelectExpression(p));
  }
  return list;
}

function parseSelectExpression(p: BaseParser): ParsedExpression {
  const expr = parseExpression(p);

  if (p.match(TokenType.AS)) {
    const aliasToken = p.expectIdentifier(`Expected alias after AS`);
    return { ...expr, alias: aliasToken.value };
  }

  if (p.checkIdentifier() && !p.isClauseKeyword()) {
    const aliasToken = p.advance();
    return { ...expr, alias: aliasToken.value };
  }

  return expr;
}

// ===========================================================================
// CTE
// ===========================================================================

function parseCTE(p: BaseParser): CTEMap {
  const map: Record<string, { query: SelectStatement; aliases: string[] }> = {};
  if (!p.match(TokenType.WITH)) return { map };

  if (p.check(TokenType.IDENTIFIER) && p.peek().value.toLowerCase() === 'recursive') {
    p.error("WITH RECURSIVE is not supported");
  }

  do {
    const nameToken = p.expect(TokenType.IDENTIFIER, `Expected CTE name after WITH`);
    const name = nameToken.value;

    const aliases: string[] = [];
    if (p.match(TokenType.LEFT_PAREN)) {
      do {
        aliases.push(p.expect(TokenType.IDENTIFIER, `Expected column alias`).value);
      } while (p.match(TokenType.COMMA));
      p.expect(TokenType.RIGHT_PAREN);
    }

    p.expect(TokenType.AS, `Expected AS after CTE name '${name}'`);
    p.expect(TokenType.LEFT_PAREN, `Expected '(' after AS in CTE '${name}'`);
    const query = p.parseSelectStatement() as SelectStatement;
    p.expect(TokenType.RIGHT_PAREN, `Expected ')' to close CTE '${name}'`);

    map[name] = { query, aliases };
  } while (p.match(TokenType.COMMA));

  return { map };
}

// ===========================================================================
// FROM / JOIN
// ===========================================================================

function parseFrom(p: BaseParser): TableRef {
  let table = parseTableRefPrimary(p);

  while (isJoinKeyword(p)) {
    table = parseJoin(p, table);
  }

  return table;
}

function parseTableRefPrimary(p: BaseParser): TableRef {
  // Subquery in FROM
  if (p.check(TokenType.LEFT_PAREN)) {
    p.advance();
    const subquery = p.parseSelectStatement() as SelectStatement;
    p.expect(TokenType.RIGHT_PAREN, `Expected ')' after subquery`);

    let alias: string | null = null;
    if (p.match(TokenType.AS)) {
      alias = p.expectIdentifier(`Expected alias after AS`).value;
    } else if (p.checkIdentifier()) {
      alias = p.advance().value;
    }

    return {
      type: TableRefType.SUBQUERY,
      subquery,
      alias,
      column_name_alias: [],
    } satisfies SubqueryRef;
  }

  const nameToken = p.expect(TokenType.IDENTIFIER, `Expected table name after FROM, got '${p.peek().value || 'EOF'}'`);

  let alias: string | null = null;
  if (p.match(TokenType.AS)) {
    alias = p.expectIdentifier(`Expected alias after AS`).value;
  } else if (p.checkIdentifier() && !isJoinKeyword(p) && !p.isClauseKeyword()) {
    alias = p.advance().value;
  }

  return {
    type: TableRefType.BASE_TABLE,
    table_name: nameToken.value,
    alias,
    schema_name: null,
  } satisfies BaseTableRef;
}

function isJoinKeyword(p: BaseParser): boolean {
  const t = p.peek().type;
  if (t === TokenType.JOIN || t === TokenType.CROSS) return true;
  if (t === TokenType.INNER) return true;
  if (t === TokenType.LEFT) return true;
  if (t === TokenType.RIGHT) return true;
  return false;
}

function parseJoin(p: BaseParser, left: TableRef): JoinRef {
  let joinType: JoinType;

  if (p.match(TokenType.CROSS)) {
    p.expect(TokenType.JOIN, `Expected JOIN after CROSS`);
    const right = parseTableRefPrimary(p);
    return {
      type: TableRefType.JOIN,
      left,
      right,
      condition: null,
      join_type: JoinType.CROSS,
      using_columns: [],
    };
  }

  if (p.match(TokenType.LEFT)) {
    p.match(TokenType.OUTER);
    joinType = JoinType.LEFT;
  } else if (p.match(TokenType.RIGHT)) {
    p.match(TokenType.OUTER);
    joinType = JoinType.RIGHT;
  } else if (p.match(TokenType.INNER)) {
    joinType = JoinType.INNER;
  } else {
    joinType = JoinType.INNER; // plain JOIN = INNER
  }

  p.expect(TokenType.JOIN, `Expected JOIN`);
  const right = parseTableRefPrimary(p);

  let condition: ParsedExpression | null = null;
  const using_columns: string[] = [];

  if (p.match(TokenType.ON)) {
    condition = parseExpression(p);
  } else if (p.match(TokenType.USING)) {
    p.expect(TokenType.LEFT_PAREN);
    do {
      using_columns.push(p.expect(TokenType.IDENTIFIER, `Expected column name in USING`).value);
    } while (p.match(TokenType.COMMA));
    p.expect(TokenType.RIGHT_PAREN);
  }

  return {
    type: TableRefType.JOIN,
    left,
    right,
    condition,
    join_type: joinType,
    using_columns,
  };
}

// ===========================================================================
// GROUP BY / ORDER BY / LIMIT
// ===========================================================================

function parseGroupBy(p: BaseParser): GroupByNode {
  const group_expressions: ParsedExpression[] = [];

  if (p.match(TokenType.GROUP)) {
    p.expect(TokenType.BY, `Expected BY after GROUP`);
    do {
      group_expressions.push(parseExpression(p));
    } while (p.match(TokenType.COMMA));
  }

  return { group_expressions };
}

function parseOrderBy(p: BaseParser): OrderByNode[] {
  p.expect(TokenType.ORDER);
  p.expect(TokenType.BY, `Expected BY after ORDER`);

  const orders: OrderByNode[] = [];
  do {
    const expression = parseExpression(p);
    let type = OrderType.ASCENDING;
    if (p.match(TokenType.ASC)) {
      type = OrderType.ASCENDING;
    } else if (p.match(TokenType.DESC)) {
      type = OrderType.DESCENDING;
    }

    let null_order = type === OrderType.ASCENDING
      ? OrderByNullType.NULLS_LAST
      : OrderByNullType.NULLS_FIRST;

    if (p.match(TokenType.NULLS)) {
      if (p.match(TokenType.FIRST)) {
        null_order = OrderByNullType.NULLS_FIRST;
      } else if (p.match(TokenType.LAST)) {
        null_order = OrderByNullType.NULLS_LAST;
      } else {
        p.error(`Expected FIRST or LAST after NULLS`);
      }
    }

    orders.push({ type, null_order, expression });
  } while (p.match(TokenType.COMMA));

  return orders;
}

function parseLimitOffset(p: BaseParser): LimitModifier {
  let limit: ParsedExpression | null = null;
  let offset: ParsedExpression | null = null;

  if (p.match(TokenType.LIMIT)) {
    limit = parseExpression(p);
  }
  if (p.match(TokenType.OFFSET)) {
    offset = parseExpression(p);
  }
  if (limit === null && p.match(TokenType.LIMIT)) {
    limit = parseExpression(p);
  }

  return { type: ResultModifierType.LIMIT_MODIFIER, limit, offset };
}
