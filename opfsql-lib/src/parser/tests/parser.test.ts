import { describe, it, expect } from 'vitest';
import { Parser } from '../parser/index.js';
import {
  StatementType, ExpressionClass, ExpressionType,
  TableRefType, JoinType, ResultModifierType,
  OrderType, OrderByNullType, LogicalTypeId,
  SelectStatement, SetOperationStatement, SetOperationType, SetOperationNode,
  InsertStatement, UpdateStatement, DeleteStatement,
  CreateTableStatement, CreateIndexStatement,
  AlterTableStatement, AlterType,
  DropStatement, DropType,
  TransactionStatement, TransactionType,
  SelectNode, ColumnRefExpression, ConstantExpression,
  ComparisonExpression, ConjunctionExpression, OperatorExpression,
  BetweenExpression, FunctionExpression, SubqueryExpression,
  CaseExpression, CastExpression, StarExpression,
  BaseTableRef, JoinRef, SubqueryRef,
  OrderModifier, LimitModifier, DistinctModifier,
  ParseError,
  OnConflictClause, OnConflictUpdate,
} from '../types.js';

const parser = new Parser();

function parse(sql: string) {
  return parser.parse(sql);
}

function parseOne(sql: string) {
  const stmts = parse(sql);
  expect(stmts).toHaveLength(1);
  return stmts[0];
}

function parseSelect(sql: string): SelectStatement {
  const stmt = parseOne(sql);
  expect(stmt.type).toBe(StatementType.SELECT_STATEMENT);
  return stmt as SelectStatement;
}

function selectNode(sql: string): SelectNode {
  const stmt = parseSelect(sql);
  expect(stmt.node.type).toBe('SELECT_NODE');
  return stmt.node as SelectNode;
}

// ============================================================================
// SELECT
// ============================================================================

describe('SELECT', () => {
  it('parses SELECT * FROM users', () => {
    const node = selectNode('SELECT * FROM users');
    expect(node.select_list).toHaveLength(1);
    expect(node.select_list[0].expression_class).toBe(ExpressionClass.STAR);
    expect((node.select_list[0] as StarExpression).table_name).toBeNull();
    expect(node.from_table).not.toBeNull();
    expect(node.from_table!.type).toBe(TableRefType.BASE_TABLE);
    expect((node.from_table as BaseTableRef).table_name).toBe('users');
  });

  it('parses SELECT id, name FROM users', () => {
    const node = selectNode('SELECT id, name FROM users');
    expect(node.select_list).toHaveLength(2);
    expect((node.select_list[0] as ColumnRefExpression).column_names).toEqual(['id']);
    expect((node.select_list[1] as ColumnRefExpression).column_names).toEqual(['name']);
  });

  it('parses SELECT u.name FROM users u', () => {
    const node = selectNode('SELECT u.name FROM users u');
    const col = node.select_list[0] as ColumnRefExpression;
    expect(col.column_names).toEqual(['u', 'name']);
    expect((node.from_table as BaseTableRef).alias).toBe('u');
  });

  it('parses SELECT with alias (AS)', () => {
    const node = selectNode('SELECT id AS user_id FROM users');
    expect(node.select_list[0].alias).toBe('user_id');
  });

  it('parses SELECT with implicit alias', () => {
    const node = selectNode('SELECT id user_id FROM users');
    expect(node.select_list[0].alias).toBe('user_id');
  });

  it('parses SELECT with quoted implicit alias', () => {
    const node = selectNode('SELECT 1 "my alias"');
    expect(node.select_list[0].alias).toBe('my alias');
  });

  it('parses quoted identifier as column ref', () => {
    const node = selectNode('SELECT t."my col" FROM t');
    const col = node.select_list[0] as ColumnRefExpression;
    expect(col.column_names).toEqual(['t', 'my col']);
  });

  it('parses SELECT without FROM', () => {
    const node = selectNode('SELECT 1');
    expect(node.from_table).toBeNull();
    const val = node.select_list[0] as ConstantExpression;
    expect(val.value.value).toBe(1);
  });

  it('parses table.*', () => {
    const node = selectNode('SELECT u.* FROM users u');
    const star = node.select_list[0] as StarExpression;
    expect(star.expression_class).toBe(ExpressionClass.STAR);
    expect(star.table_name).toBe('u');
  });
});

// ============================================================================
// WHERE
// ============================================================================

describe('WHERE', () => {
  it('parses simple comparison', () => {
    const node = selectNode('SELECT * FROM users WHERE age > 18');
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.expression_class).toBe(ExpressionClass.COMPARISON);
    expect(cmp.type).toBe(ExpressionType.COMPARE_GREATERTHAN);
    expect((cmp.left as ColumnRefExpression).column_names).toEqual(['age']);
    expect((cmp.right as ConstantExpression).value.value).toBe(18);
  });

  it('parses BETWEEN', () => {
    const node = selectNode('SELECT * FROM users WHERE age BETWEEN 18 AND 65');
    const expr = node.where_clause as BetweenExpression;
    expect(expr.expression_class).toBe(ExpressionClass.BETWEEN);
    expect((expr.lower as ConstantExpression).value.value).toBe(18);
    expect((expr.upper as ConstantExpression).value.value).toBe(65);
  });

  it('parses LIKE', () => {
    const node = selectNode("SELECT * FROM users WHERE name LIKE '%ivan%'");
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_LIKE);
  });

  it('parses NOT LIKE', () => {
    const node = selectNode("SELECT * FROM users WHERE name NOT LIKE '%test%'");
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_NOT_LIKE);
  });

  it('parses IS NULL', () => {
    const node = selectNode('SELECT * FROM users WHERE email IS NULL');
    const op = node.where_clause as OperatorExpression;
    expect(op.type).toBe(ExpressionType.OPERATOR_IS_NULL);
  });

  it('parses IS NOT NULL', () => {
    const node = selectNode('SELECT * FROM users WHERE email IS NOT NULL');
    const op = node.where_clause as OperatorExpression;
    expect(op.type).toBe(ExpressionType.OPERATOR_IS_NOT_NULL);
  });

  it('parses IN with values', () => {
    const node = selectNode('SELECT * FROM users WHERE id IN (1, 2, 3)');
    const op = node.where_clause as OperatorExpression;
    expect(op.type).toBe(ExpressionType.OPERATOR_IN);
    // children[0] is the left side (id), children[1..3] are the values
    expect(op.children).toHaveLength(4);
  });

  it('parses NOT IN', () => {
    const node = selectNode('SELECT * FROM users WHERE id NOT IN (1, 2)');
    const op = node.where_clause as OperatorExpression;
    expect(op.type).toBe(ExpressionType.OPERATOR_NOT_IN);
  });

  it('parses IN with subquery', () => {
    const node = selectNode('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)');
    const sub = node.where_clause as SubqueryExpression;
    expect(sub.expression_class).toBe(ExpressionClass.SUBQUERY);
    expect(sub.subquery_type).toBe('ANY');
    expect(sub.child).not.toBeNull();
    const col = sub.child as ColumnRefExpression;
    expect(col.column_names).toEqual(['id']);
  });

  it('parses NOT IN with subquery', () => {
    const node = selectNode('SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)');
    const notOp = node.where_clause as OperatorExpression;
    expect(notOp.type).toBe(ExpressionType.OPERATOR_NOT);
    const sub = notOp.children[0] as SubqueryExpression;
    expect(sub.subquery_type).toBe('ANY');
    expect(sub.child).not.toBeNull();
    const col = sub.child as ColumnRefExpression;
    expect(col.column_names).toEqual(['id']);
  });

  it('parses EXISTS', () => {
    const node = selectNode('SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)');
    const sub = node.where_clause as SubqueryExpression;
    expect(sub.expression_class).toBe(ExpressionClass.SUBQUERY);
    expect(sub.subquery_type).toBe('EXISTS');
  });

  it('parses NOT EXISTS', () => {
    const node = selectNode('SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders)');
    const sub = node.where_clause as SubqueryExpression;
    expect(sub.subquery_type).toBe('NOT_EXISTS');
  });

  it('parses AND / OR', () => {
    const node = selectNode('SELECT * FROM users WHERE age > 18 AND active = 1 OR name = \'admin\'');
    // OR is lower precedence, so top level is OR
    const or = node.where_clause as ConjunctionExpression;
    expect(or.type).toBe(ExpressionType.CONJUNCTION_OR);
    const and = or.children[0] as ConjunctionExpression;
    expect(and.type).toBe(ExpressionType.CONJUNCTION_AND);
  });

  it('flattens chained AND into single conjunction', () => {
    const node = selectNode('SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3');
    const conj = node.where_clause as ConjunctionExpression;
    expect(conj.type).toBe(ExpressionType.CONJUNCTION_AND);
    expect(conj.children).toHaveLength(3);
  });

  it('flattens chained OR into single conjunction', () => {
    const node = selectNode('SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3');
    const conj = node.where_clause as ConjunctionExpression;
    expect(conj.type).toBe(ExpressionType.CONJUNCTION_OR);
    expect(conj.children).toHaveLength(3);
  });

  it('parses NOT', () => {
    const node = selectNode('SELECT * FROM users WHERE NOT active = 1');
    const notExpr = node.where_clause as OperatorExpression;
    expect(notExpr.type).toBe(ExpressionType.OPERATOR_NOT);
  });

  it('parses parenthesized conditions', () => {
    const node = selectNode('SELECT * FROM users WHERE (age > 18 OR age < 10) AND active = 1');
    const and = node.where_clause as ConjunctionExpression;
    expect(and.type).toBe(ExpressionType.CONJUNCTION_AND);
    const or = and.children[0] as ConjunctionExpression;
    expect(or.type).toBe(ExpressionType.CONJUNCTION_OR);
  });

  it('parses comparison operators', () => {
    const ops = [
      ['=', ExpressionType.COMPARE_EQUAL],
      ['!=', ExpressionType.COMPARE_NOTEQUAL],
      ['<>', ExpressionType.COMPARE_NOTEQUAL],
      ['<', ExpressionType.COMPARE_LESSTHAN],
      ['<=', ExpressionType.COMPARE_LESSTHANOREQUALTO],
      ['>', ExpressionType.COMPARE_GREATERTHAN],
      ['>=', ExpressionType.COMPARE_GREATERTHANOREQUALTO],
    ] as const;

    for (const [op, expected] of ops) {
      const node = selectNode(`SELECT * FROM t WHERE a ${op} 1`);
      const cmp = node.where_clause as ComparisonExpression;
      expect(cmp.type).toBe(expected);
    }
  });
});

// ============================================================================
// JOIN
// ============================================================================

describe('JOIN', () => {
  it('parses LEFT JOIN', () => {
    const node = selectNode('SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id');
    const join = node.from_table as JoinRef;
    expect(join.type).toBe(TableRefType.JOIN);
    expect(join.join_type).toBe(JoinType.LEFT);
    expect((join.left as BaseTableRef).table_name).toBe('users');
    expect((join.right as BaseTableRef).table_name).toBe('orders');
    expect(join.condition).not.toBeNull();
  });

  it('parses RIGHT JOIN', () => {
    const node = selectNode('SELECT * FROM a RIGHT JOIN b ON a.id = b.id');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.RIGHT);
  });

  it('parses INNER JOIN', () => {
    const node = selectNode('SELECT * FROM a INNER JOIN b ON a.id = b.id');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.INNER);
  });

  it('parses plain JOIN as INNER', () => {
    const node = selectNode('SELECT * FROM a JOIN b ON a.id = b.id');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.INNER);
  });

  it('parses CROSS JOIN', () => {
    const node = selectNode('SELECT * FROM a CROSS JOIN b');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.CROSS);
    expect(join.condition).toBeNull();
  });

  it('parses JOIN with USING', () => {
    const node = selectNode('SELECT * FROM a JOIN b USING (id)');
    const join = node.from_table as JoinRef;
    expect(join.using_columns).toEqual(['id']);
  });

});

// ============================================================================
// GROUP BY / HAVING
// ============================================================================

describe('GROUP BY / HAVING', () => {
  it('parses GROUP BY with HAVING', () => {
    const node = selectNode('SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5');
    expect(node.groups.group_expressions).toHaveLength(1);
    expect((node.groups.group_expressions[0] as ColumnRefExpression).column_names).toEqual(['dept']);
    expect(node.having).not.toBeNull();
    const cmp = node.having as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_GREATERTHAN);
  });
});

// ============================================================================
// ORDER BY / LIMIT / OFFSET / DISTINCT
// ============================================================================

describe('ORDER BY / LIMIT / OFFSET / DISTINCT', () => {
  it('parses ORDER BY', () => {
    const node = selectNode('SELECT * FROM users ORDER BY age DESC, name ASC');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders).toHaveLength(2);
    expect(orderMod.orders[0].type).toBe(OrderType.DESCENDING);
    expect(orderMod.orders[1].type).toBe(OrderType.ASCENDING);
  });

  it('parses ORDER BY with NULLS FIRST/LAST', () => {
    const node = selectNode('SELECT * FROM users ORDER BY age ASC NULLS FIRST');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].null_order).toBe(OrderByNullType.NULLS_FIRST);
  });

  it('parses LIMIT and OFFSET', () => {
    const node = selectNode('SELECT * FROM users LIMIT 10 OFFSET 20');
    const limitMod = node.modifiers.find(m => m.type === ResultModifierType.LIMIT_MODIFIER) as LimitModifier;
    expect((limitMod.limit as ConstantExpression).value.value).toBe(10);
    expect((limitMod.offset as ConstantExpression).value.value).toBe(20);
  });

  it('parses DISTINCT', () => {
    const node = selectNode('SELECT DISTINCT name FROM users');
    const distinctMod = node.modifiers.find(m => m.type === ResultModifierType.DISTINCT_MODIFIER) as DistinctModifier;
    expect(distinctMod).toBeDefined();
  });
});

// ============================================================================
// CTE
// ============================================================================

describe('CTE', () => {
  it('parses WITH ... AS', () => {
    const stmt = parseSelect('WITH active AS (SELECT * FROM users WHERE active = 1) SELECT * FROM active');
    const node = stmt.node as SelectNode;
    expect(node.cte_map.map['active']).toBeDefined();
    const cteQuery = node.cte_map.map['active'].query;
    expect(cteQuery.type).toBe(StatementType.SELECT_STATEMENT);
  });
});

// ============================================================================
// Subquery in FROM
// ============================================================================

describe('Subquery in FROM', () => {
  it('parses subquery with alias', () => {
    const node = selectNode('SELECT * FROM (SELECT * FROM users) AS sub');
    expect(node.from_table!.type).toBe(TableRefType.SUBQUERY);
    expect((node.from_table as SubqueryRef).alias).toBe('sub');
  });
});

// ============================================================================
// UNION
// ============================================================================

describe('UNION', () => {
  it('parses UNION ALL', () => {
    const stmt = parseOne('SELECT * FROM users UNION ALL SELECT * FROM admins');
    expect(stmt.type).toBe(StatementType.SELECT_STATEMENT);
    const setStmt = stmt as SetOperationStatement;
    expect(setStmt.node.type).toBe('SET_OPERATION_NODE');
    const setNode = setStmt.node;
    expect(setNode.set_op_type).toBe(SetOperationType.UNION_ALL);
  });

  it('parses UNION (without ALL)', () => {
    const stmt = parseOne('SELECT 1 UNION SELECT 2');
    const setStmt = stmt as SetOperationStatement;
    expect(setStmt.node.set_op_type).toBe(SetOperationType.UNION);
  });

  it('parses chained UNION (A UNION B UNION ALL C)', () => {
    const stmt = parseOne('SELECT 1 UNION SELECT 2 UNION ALL SELECT 3');
    const setStmt = stmt as SetOperationStatement;
    const outer = setStmt.node;
    expect(outer.type).toBe('SET_OPERATION_NODE');
    expect(outer.set_op_type).toBe(SetOperationType.UNION_ALL);
    // left is (SELECT 1 UNION SELECT 2)
    expect(outer.left.type).toBe('SET_OPERATION_NODE');
    const inner = outer.left as typeof outer;
    expect(inner.set_op_type).toBe(SetOperationType.UNION);
    // right is SELECT 3
    expect(outer.right.type).toBe('SELECT_NODE');
  });
});

// ============================================================================
// Functions / Expressions
// ============================================================================

describe('Expressions', () => {
  it('parses COUNT(*)', () => {
    const node = selectNode('SELECT COUNT(*) FROM users');
    const func = node.select_list[0] as FunctionExpression;
    expect(func.expression_class).toBe(ExpressionClass.FUNCTION);
    expect(func.function_name).toBe('count');
    expect(func.is_star).toBe(true);
  });

  it('parses function with DISTINCT', () => {
    const node = selectNode('SELECT COUNT(DISTINCT name) FROM users');
    const func = node.select_list[0] as FunctionExpression;
    expect(func.distinct).toBe(true);
  });

  it('parses CASE expression', () => {
    const node = selectNode("SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users");
    const caseExpr = node.select_list[0] as CaseExpression;
    expect(caseExpr.expression_class).toBe(ExpressionClass.CASE);
    expect(caseExpr.case_checks).toHaveLength(1);
    expect(caseExpr.else_expr).not.toBeNull();
  });

  it('parses simple CASE expression', () => {
    const node = selectNode("SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END FROM users");
    const caseExpr = node.select_list[0] as CaseExpression;
    expect(caseExpr.expression_class).toBe(ExpressionClass.CASE);
    expect(caseExpr.case_checks).toHaveLength(2);
    // Simple CASE is desugared to searched CASE with equality comparisons
    const check0 = caseExpr.case_checks[0].when_expr as ComparisonExpression;
    expect(check0.expression_class).toBe(ExpressionClass.COMPARISON);
    expect(check0.type).toBe(ExpressionType.COMPARE_EQUAL);
    expect(caseExpr.else_expr).not.toBeNull();
  });

  it('parses CAST expression', () => {
    const node = selectNode('SELECT CAST(age AS TEXT) FROM users');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.expression_class).toBe(ExpressionClass.CAST);
    expect(cast.cast_type.id).toBe(LogicalTypeId.VARCHAR);
  });

  it('parses arithmetic expressions', () => {
    const node = selectNode('SELECT a + b * c FROM t');
    // + is lower precedence than *, so top is OPERATOR_ADD
    const add = node.select_list[0] as OperatorExpression;
    expect(add.type).toBe(ExpressionType.OPERATOR_ADD);
    const mul = add.children[1] as OperatorExpression;
    expect(mul.type).toBe(ExpressionType.OPERATOR_MULTIPLY);
  });

  it('parses || string concatenation', () => {
    const node = selectNode("SELECT 'a' || 'b'");
    const op = node.select_list[0] as OperatorExpression;
    expect(op.expression_class).toBe(ExpressionClass.OPERATOR);
    expect(op.type).toBe(ExpressionType.OPERATOR_CONCAT);
    expect(op.children).toHaveLength(2);
  });

  it('|| is left-associative', () => {
    const node = selectNode("SELECT a || b || c FROM t");
    // (a || b) || c — outer is CONCAT, left child is also CONCAT
    const outer = node.select_list[0] as OperatorExpression;
    expect(outer.type).toBe(ExpressionType.OPERATOR_CONCAT);
    const inner = outer.children[0] as OperatorExpression;
    expect(inner.type).toBe(ExpressionType.OPERATOR_CONCAT);
  });

  it('|| has lower precedence than arithmetic', () => {
    const node = selectNode("SELECT a + 1 || b FROM t");
    // (a + 1) || b — top is CONCAT
    const concat = node.select_list[0] as OperatorExpression;
    expect(concat.type).toBe(ExpressionType.OPERATOR_CONCAT);
    const add = concat.children[0] as OperatorExpression;
    expect(add.type).toBe(ExpressionType.OPERATOR_ADD);
  });

  it('parses unary minus', () => {
    const node = selectNode('SELECT -1');
    const neg = node.select_list[0] as OperatorExpression;
    expect(neg.type).toBe(ExpressionType.OPERATOR_NEGATE);
  });

  it('parses string literal', () => {
    const node = selectNode("SELECT 'hello'");
    const str = node.select_list[0] as ConstantExpression;
    expect(str.value.value).toBe('hello');
    expect(str.value.type.id).toBe(LogicalTypeId.VARCHAR);
  });

  it('parses boolean literals', () => {
    const node = selectNode('SELECT TRUE, FALSE');
    expect((node.select_list[0] as ConstantExpression).value.value).toBe(true);
    expect((node.select_list[1] as ConstantExpression).value.value).toBe(false);
  });

  it('parses NULL', () => {
    const node = selectNode('SELECT NULL');
    const n = node.select_list[0] as ConstantExpression;
    expect(n.value.is_null).toBe(true);
    expect(n.value.value).toBeNull();
  });

  it('parses scalar subquery', () => {
    const node = selectNode('SELECT (SELECT 1)');
    const sub = node.select_list[0] as SubqueryExpression;
    expect(sub.subquery_type).toBe('SCALAR');
  });
});

// ============================================================================
// DML
// ============================================================================

describe('DML', () => {
  it('parses INSERT with columns and values', () => {
    const stmt = parseOne("INSERT INTO users (name, age) VALUES ('Ivan', 25)") as InsertStatement;
    expect(stmt.type).toBe(StatementType.INSERT_STATEMENT);
    expect(stmt.table).toBe('users');
    expect(stmt.columns).toEqual(['name', 'age']);
    expect(stmt.values).toHaveLength(1);
    expect(stmt.values[0]).toHaveLength(2);
  });

  it('parses INSERT with multiple rows', () => {
    const stmt = parseOne("INSERT INTO users VALUES ('Ivan', 25), ('Anna', 22)") as InsertStatement;
    expect(stmt.values).toHaveLength(2);
  });

  it('parses INSERT ... SELECT', () => {
    const stmt = parseOne('INSERT INTO archive SELECT * FROM users WHERE active = 0') as InsertStatement;
    expect(stmt.select_statement).not.toBeNull();
    expect(stmt.values).toHaveLength(0);
    expect(stmt.onConflict).toBeNull();
  });

  it('parses INSERT without ON CONFLICT has null onConflict', () => {
    const stmt = parseOne("INSERT INTO users (id, name) VALUES (1, 'Alice')") as InsertStatement;
    expect(stmt.onConflict).toBeNull();
  });

  it('parses INSERT ... ON CONFLICT (col) DO NOTHING', () => {
    const stmt = parseOne(
      "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING"
    ) as InsertStatement;
    expect(stmt.onConflict).not.toBeNull();
    expect(stmt.onConflict!.conflictTarget).toEqual(['id']);
    expect(stmt.onConflict!.action).toBe('NOTHING');
  });

  it('parses INSERT ... ON CONFLICT DO NOTHING (no target)', () => {
    const stmt = parseOne(
      "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT DO NOTHING"
    ) as InsertStatement;
    expect(stmt.onConflict).not.toBeNull();
    expect(stmt.onConflict!.conflictTarget).toBeNull();
    expect(stmt.onConflict!.action).toBe('NOTHING');
  });

  it('parses INSERT ... ON CONFLICT with multi-column target', () => {
    const stmt = parseOne(
      "INSERT INTO t (a, b, c) VALUES (1, 2, 3) ON CONFLICT (a, b) DO NOTHING"
    ) as InsertStatement;
    expect(stmt.onConflict!.conflictTarget).toEqual(['a', 'b']);
  });

  it('parses INSERT ... ON CONFLICT DO UPDATE SET', () => {
    const stmt = parseOne(
      "INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO UPDATE SET name = excluded.name"
    ) as InsertStatement;
    expect(stmt.onConflict).not.toBeNull();
    expect(stmt.onConflict!.conflictTarget).toEqual(['id']);
    const action = stmt.onConflict!.action as OnConflictUpdate;
    expect(action.type).toBe('UPDATE');
    expect(action.setClauses).toHaveLength(1);
    expect(action.setClauses[0].column).toBe('name');
    // excluded.name is a ColumnRef with two names
    const ref = action.setClauses[0].value as ColumnRefExpression;
    expect(ref.expression_class).toBe(ExpressionClass.COLUMN_REF);
    expect(ref.column_names).toEqual(['excluded', 'name']);
  });

  it('parses INSERT ... ON CONFLICT DO UPDATE SET with WHERE', () => {
    const stmt = parseOne(
      "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30) ON CONFLICT (id) DO UPDATE SET age = excluded.age WHERE age < excluded.age"
    ) as InsertStatement;
    const action = stmt.onConflict!.action as OnConflictUpdate;
    expect(action.type).toBe('UPDATE');
    expect(action.setClauses).toHaveLength(1);
    expect(action.whereClause).not.toBeNull();
  });

  it('parses INSERT ... ON CONFLICT DO UPDATE with multiple SET clauses', () => {
    const stmt = parseOne(
      "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30) ON CONFLICT (id) DO UPDATE SET name = excluded.name, age = age + 1"
    ) as InsertStatement;
    const action = stmt.onConflict!.action as OnConflictUpdate;
    expect(action.setClauses).toHaveLength(2);
    expect(action.setClauses[0].column).toBe('name');
    expect(action.setClauses[1].column).toBe('age');
  });

  it('parses INSERT ... SELECT ... ON CONFLICT DO NOTHING', () => {
    const stmt = parseOne(
      'INSERT INTO archive SELECT * FROM users ON CONFLICT (id) DO NOTHING'
    ) as InsertStatement;
    expect(stmt.select_statement).not.toBeNull();
    expect(stmt.onConflict).not.toBeNull();
    expect(stmt.onConflict!.action).toBe('NOTHING');
  });

  it('parses UPDATE with SET and WHERE', () => {
    const stmt = parseOne('UPDATE users SET age = 26 WHERE id = 1') as UpdateStatement;
    expect(stmt.type).toBe(StatementType.UPDATE_STATEMENT);
    expect(stmt.table).toBe('users');
    expect(stmt.set_clauses).toHaveLength(1);
    expect(stmt.set_clauses[0].column).toBe('age');
    expect(stmt.where_clause).not.toBeNull();
  });

  it('parses UPDATE with multiple SET clauses', () => {
    const stmt = parseOne("UPDATE users SET age = age + 1, name = 'Ivan'") as UpdateStatement;
    expect(stmt.set_clauses).toHaveLength(2);
    // age = age + 1: value should be an operator expression
    const addExpr = stmt.set_clauses[0].value as OperatorExpression;
    expect(addExpr.type).toBe(ExpressionType.OPERATOR_ADD);
  });

  it('parses DELETE with WHERE', () => {
    const stmt = parseOne('DELETE FROM users WHERE id = 1') as DeleteStatement;
    expect(stmt.type).toBe(StatementType.DELETE_STATEMENT);
    expect(stmt.table).toBe('users');
    expect(stmt.where_clause).not.toBeNull();
  });

  it('parses DELETE without WHERE', () => {
    const stmt = parseOne('DELETE FROM users') as DeleteStatement;
    expect(stmt.where_clause).toBeNull();
  });
});

// ============================================================================
// DDL
// ============================================================================

describe('DDL', () => {
  it('parses CREATE TABLE', () => {
    const stmt = parseOne('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)') as CreateTableStatement;
    expect(stmt.type).toBe(StatementType.CREATE_TABLE_STATEMENT);
    expect(stmt.table).toBe('users');
    expect(stmt.columns).toHaveLength(2);
    expect(stmt.columns[0].name).toBe('id');
    expect(stmt.columns[0].is_primary_key).toBe(true);
    expect(stmt.columns[1].name).toBe('name');
    expect(stmt.columns[1].is_not_null).toBe(true);
  });

  it('parses CREATE TABLE IF NOT EXISTS', () => {
    const stmt = parseOne('CREATE TABLE IF NOT EXISTS users (id INTEGER)') as CreateTableStatement;
    expect(stmt.if_not_exists).toBe(true);
  });

  it('parses CREATE TABLE with table-level PRIMARY KEY', () => {
    const stmt = parseOne('CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))') as CreateTableStatement;
    expect(stmt.primary_key).toEqual(['a', 'b']);
  });

  it('parses CREATE TABLE with FOREIGN KEY', () => {
    const stmt = parseOne('CREATE TABLE orders (id INTEGER, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))') as CreateTableStatement;
    expect(stmt.foreign_keys).toHaveLength(1);
    expect(stmt.foreign_keys[0].ref_table).toBe('users');
  });

  it('parses CREATE TABLE with DEFAULT', () => {
    const stmt = parseOne("CREATE TABLE t (active BOOLEAN DEFAULT TRUE)") as CreateTableStatement;
    expect(stmt.columns[0].default_value).not.toBeNull();
  });

  it('parses CREATE TABLE with DEFAULT -1', () => {
    const stmt = parseOne("CREATE TABLE t (score INTEGER DEFAULT -1 NOT NULL)") as CreateTableStatement;
    const def = stmt.columns[0].default_value as OperatorExpression;
    expect(def.type).toBe(ExpressionType.OPERATOR_NEGATE);
    expect(stmt.columns[0].is_not_null).toBe(true);
  });

  it('parses CREATE TABLE with UNIQUE', () => {
    const stmt = parseOne('CREATE TABLE t (email TEXT UNIQUE)') as CreateTableStatement;
    expect(stmt.columns[0].is_unique).toBe(true);
  });

  it('parses CREATE TABLE with AUTOINCREMENT', () => {
    const stmt = parseOne('CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)') as CreateTableStatement;
    expect(stmt.columns[0].is_autoincrement).toBe(true);
    expect(stmt.columns[0].is_primary_key).toBe(true);
    expect(stmt.columns[1].is_autoincrement).toBe(false);
  });

  it('parses AUTOINCREMENT with NOT NULL', () => {
    const stmt = parseOne('CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL)') as CreateTableStatement;
    expect(stmt.columns[0].is_autoincrement).toBe(true);
    expect(stmt.columns[0].is_not_null).toBe(true);
  });

  it('column without AUTOINCREMENT defaults to false', () => {
    const stmt = parseOne('CREATE TABLE t (id INTEGER PRIMARY KEY)') as CreateTableStatement;
    expect(stmt.columns[0].is_autoincrement).toBe(false);
  });

  it('parses CREATE INDEX', () => {
    const stmt = parseOne('CREATE INDEX idx_name ON users (name)') as CreateIndexStatement;
    expect(stmt.type).toBe(StatementType.CREATE_INDEX_STATEMENT);
    expect(stmt.index_name).toBe('idx_name');
    expect(stmt.table_name).toBe('users');
    expect(stmt.columns).toEqual(['name']);
    expect(stmt.is_unique).toBe(false);
  });

  it('parses CREATE UNIQUE INDEX', () => {
    const stmt = parseOne('CREATE UNIQUE INDEX idx ON users (email)') as CreateIndexStatement;
    expect(stmt.is_unique).toBe(true);
  });

  it('parses CREATE INDEX IF NOT EXISTS', () => {
    const stmt = parseOne('CREATE INDEX IF NOT EXISTS idx ON t (c)') as CreateIndexStatement;
    expect(stmt.if_not_exists).toBe(true);
  });

  it('parses ALTER TABLE ADD COLUMN', () => {
    const stmt = parseOne('ALTER TABLE users ADD COLUMN email TEXT') as AlterTableStatement;
    expect(stmt.type).toBe(StatementType.ALTER_TABLE_STATEMENT);
    expect(stmt.alter_type).toBe(AlterType.ADD_COLUMN);
    expect(stmt.column_def!.name).toBe('email');
  });

  it('parses ALTER TABLE DROP COLUMN', () => {
    const stmt = parseOne('ALTER TABLE users DROP COLUMN email') as AlterTableStatement;
    expect(stmt.alter_type).toBe(AlterType.DROP_COLUMN);
    expect(stmt.column_name).toBe('email');
  });

  it('parses DROP TABLE', () => {
    const stmt = parseOne('DROP TABLE users') as DropStatement;
    expect(stmt.type).toBe(StatementType.DROP_STATEMENT);
    expect(stmt.drop_type).toBe(DropType.TABLE);
    expect(stmt.name).toBe('users');
    expect(stmt.if_exists).toBe(false);
  });

  it('parses DROP TABLE IF EXISTS', () => {
    const stmt = parseOne('DROP TABLE IF EXISTS users') as DropStatement;
    expect(stmt.if_exists).toBe(true);
  });

  it('parses DROP INDEX', () => {
    const stmt = parseOne('DROP INDEX idx_name') as DropStatement;
    expect(stmt.drop_type).toBe(DropType.INDEX);
  });
});

// ============================================================================
// TCL
// ============================================================================

describe('TCL', () => {
  it('parses BEGIN', () => {
    const stmt = parseOne('BEGIN') as TransactionStatement;
    expect(stmt.type).toBe(StatementType.TRANSACTION_STATEMENT);
    expect(stmt.transaction_type).toBe(TransactionType.BEGIN);
  });

  it('parses BEGIN TRANSACTION', () => {
    const stmt = parseOne('BEGIN TRANSACTION') as TransactionStatement;
    expect(stmt.transaction_type).toBe(TransactionType.BEGIN);
  });

  it('parses COMMIT', () => {
    const stmt = parseOne('COMMIT') as TransactionStatement;
    expect(stmt.transaction_type).toBe(TransactionType.COMMIT);
  });

  it('parses ROLLBACK', () => {
    const stmt = parseOne('ROLLBACK') as TransactionStatement;
    expect(stmt.transaction_type).toBe(TransactionType.ROLLBACK);
  });
});

// ============================================================================
// Multiple statements
// ============================================================================

describe('Multiple statements', () => {
  it('parses two statements separated by ;', () => {
    const stmts = parse('SELECT 1; SELECT 2');
    expect(stmts).toHaveLength(2);
  });

  it('parses one statement without ;', () => {
    const stmts = parse('SELECT 1');
    expect(stmts).toHaveLength(1);
  });

  it('handles trailing semicolons', () => {
    const stmts = parse('SELECT 1;;;');
    expect(stmts).toHaveLength(1);
  });
});

// ============================================================================
// Errors
// ============================================================================

describe('Errors', () => {
  it('reports error with position for SELECT FROM', () => {
    expect(() => parse('SELECT FROM users')).toThrow(ParseError);
    expect(() => parse('SELECT FROM users')).toThrow(/line 1/);
  });

  it('reports error for SELECT * FROM (missing table)', () => {
    expect(() => parse('SELECT * FROM')).toThrow(/Expected table name/);
  });

  it('reports error for unsupported WINDOW', () => {
    expect(() => parse('SELECT WINDOW')).toThrow(/not supported/);
  });

  it('parses WITH RECURSIVE', () => {
    const stmt = parseSelect(
      'WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT * FROM r',
    );
    const node = stmt.node as SelectNode;
    expect(node.cte_map.recursive).toBe(true);
    expect(node.cte_map.map['r']).toBeDefined();
    expect(node.cte_map.map['r'].aliases).toEqual(['n']);
    // CTE body is a UNION ALL set operation
    const body = node.cte_map.map['r'].query.node;
    expect(body.type).toBe('SET_OPERATION_NODE');
    expect((body as SetOperationNode).set_op_type).toBe(SetOperationType.UNION_ALL);
  });

  it('parses WITH RECURSIVE with UNION (not ALL)', () => {
    const stmt = parseSelect(
      'WITH RECURSIVE r AS (SELECT 1 AS n UNION SELECT n + 1 FROM r WHERE n < 5) SELECT * FROM r',
    );
    const node = stmt.node as SelectNode;
    expect(node.cte_map.recursive).toBe(true);
    const body = node.cte_map.map['r'].query.node as SetOperationNode;
    expect(body.set_op_type).toBe(SetOperationType.UNION);
  });

  it('parses WITH RECURSIVE with multiple CTEs', () => {
    const stmt = parseSelect(
      'WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 5), doubled AS (SELECT n * 2 AS d FROM nums) SELECT * FROM doubled',
    );
    const node = stmt.node as SelectNode;
    expect(node.cte_map.recursive).toBe(true);
    expect(node.cte_map.map['nums']).toBeDefined();
    expect(node.cte_map.map['doubled']).toBeDefined();
  });

  it('non-recursive WITH has recursive=false', () => {
    const stmt = parseSelect('WITH a AS (SELECT 1) SELECT * FROM a');
    const node = stmt.node as SelectNode;
    expect(node.cte_map.recursive).toBe(false);
  });
});

// ============================================================================
// Complex query (integration)
// ============================================================================

describe('Complex query', () => {
  it('parses a complex CTE + JOIN + GROUP BY + HAVING + ORDER BY + LIMIT query', () => {
    const result = parse(`
      WITH active_users AS (
        SELECT id, name FROM users WHERE active = 1
      )
      SELECT u.name, COUNT(o.id) as order_count
      FROM active_users u
      LEFT JOIN orders o ON u.id = o.user_id
      WHERE o.total > 100
      GROUP BY u.name
      HAVING COUNT(o.id) > 2
      ORDER BY order_count DESC
      LIMIT 10
    `);

    expect(result).toHaveLength(1);
    expect(result[0].type).toBe(StatementType.SELECT_STATEMENT);

    const stmt = result[0] as SelectStatement;
    const node = stmt.node as SelectNode;

    // CTE
    expect(node.cte_map.map['active_users']).toBeDefined();

    // FROM is a JOIN
    expect(node.from_table!.type).toBe(TableRefType.JOIN);
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.LEFT);

    // WHERE
    expect(node.where_clause!.expression_class).toBe(ExpressionClass.COMPARISON);

    // GROUP BY
    expect(node.groups.group_expressions).toHaveLength(1);

    // HAVING
    expect(node.having).not.toBeNull();

    // Modifiers: ORDER BY + LIMIT
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod).toBeDefined();
    expect(orderMod.orders[0].type).toBe(OrderType.DESCENDING);

    const limitMod = node.modifiers.find(m => m.type === ResultModifierType.LIMIT_MODIFIER) as LimitModifier;
    expect(limitMod).toBeDefined();
    expect((limitMod.limit as ConstantExpression).value.value).toBe(10);
  });
});

// ============================================================================
// Edge cases: SELECT
// ============================================================================

describe('SELECT edge cases', () => {
  it('is case-insensitive for keywords', () => {
    const node = selectNode('select * from users');
    expect(node.select_list).toHaveLength(1);
    expect(node.select_list[0].expression_class).toBe(ExpressionClass.STAR);
    expect((node.from_table as BaseTableRef).table_name).toBe('users');
  });

  it('handles mixed-case keywords', () => {
    const node = selectNode('SeLeCt * FrOm users WhErE id = 1');
    expect(node.select_list).toHaveLength(1);
    expect(node.where_clause).not.toBeNull();
  });

  it('handles multiline SQL with extra whitespace', () => {
    const node = selectNode(`
      SELECT
        id,
        name
      FROM
        users
    `);
    expect(node.select_list).toHaveLength(2);
  });

  it('parses SELECT with expression in select list', () => {
    const node = selectNode('SELECT 1 + 2');
    const add = node.select_list[0] as OperatorExpression;
    expect(add.type).toBe(ExpressionType.OPERATOR_ADD);
  });

  it('parses table alias with AS keyword', () => {
    const node = selectNode('SELECT * FROM users AS u');
    const table = node.from_table as BaseTableRef;
    expect(table.table_name).toBe('users');
    expect(table.alias).toBe('u');
  });

  it('parses SELECT with multiple mixed expressions', () => {
    const node = selectNode("SELECT id, name, 1 + 2 AS calc, 'hello' AS greeting FROM t");
    expect(node.select_list).toHaveLength(4);
    expect(node.select_list[2].alias).toBe('calc');
    expect(node.select_list[3].alias).toBe('greeting');
  });

  it('parses SELECT with comments in SQL', () => {
    const node = selectNode(`
      SELECT id -- this is the primary key
      , name /* user name */
      FROM users
    `);
    expect(node.select_list).toHaveLength(2);
  });

  it('parses float literal in select list', () => {
    const node = selectNode('SELECT 3.14');
    const val = node.select_list[0] as ConstantExpression;
    expect(val.value.value).toBe(3.14);
    expect(val.value.type.id).toBe(LogicalTypeId.DOUBLE);
  });

  it('parses string with escaped quotes', () => {
    const node = selectNode("SELECT 'it''s a test'");
    const val = node.select_list[0] as ConstantExpression;
    expect(val.value.value).toBe("it's a test");
  });

  it('parses SELECT with only constants', () => {
    const node = selectNode("SELECT 1, 'a', TRUE, NULL");
    expect(node.select_list).toHaveLength(4);
    expect(node.from_table).toBeNull();
  });
});

// ============================================================================
// Edge cases: WHERE / Expressions
// ============================================================================

describe('WHERE edge cases', () => {
  it('parses NOT BETWEEN', () => {
    const node = selectNode('SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10');
    const notExpr = node.where_clause as OperatorExpression;
    expect(notExpr.type).toBe(ExpressionType.OPERATOR_NOT);
    const between = notExpr.children[0] as BetweenExpression;
    expect(between.expression_class).toBe(ExpressionClass.BETWEEN);
    expect((between.lower as ConstantExpression).value.value).toBe(1);
    expect((between.upper as ConstantExpression).value.value).toBe(10);
  });

  it('parses double NOT', () => {
    const node = selectNode('SELECT * FROM t WHERE NOT NOT a = 1');
    const outer = node.where_clause as OperatorExpression;
    expect(outer.type).toBe(ExpressionType.OPERATOR_NOT);
    const inner = outer.children[0] as OperatorExpression;
    expect(inner.type).toBe(ExpressionType.OPERATOR_NOT);
  });

  it('parses comparison with expressions on both sides', () => {
    const node = selectNode('SELECT * FROM t WHERE a + 1 > b * 2');
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_GREATERTHAN);
    expect((cmp.left as OperatorExpression).type).toBe(ExpressionType.OPERATOR_ADD);
    expect((cmp.right as OperatorExpression).type).toBe(ExpressionType.OPERATOR_MULTIPLY);
  });

  it('parses deeply nested parenthesized conditions', () => {
    const node = selectNode('SELECT * FROM t WHERE ((a = 1 AND b = 2) OR (c = 3 AND d = 4))');
    const or = node.where_clause as ConjunctionExpression;
    expect(or.type).toBe(ExpressionType.CONJUNCTION_OR);
    expect(or.children).toHaveLength(2);
    expect((or.children[0] as ConjunctionExpression).type).toBe(ExpressionType.CONJUNCTION_AND);
    expect((or.children[1] as ConjunctionExpression).type).toBe(ExpressionType.CONJUNCTION_AND);
  });

  it('parses IN with single value', () => {
    const node = selectNode('SELECT * FROM t WHERE id IN (1)');
    const op = node.where_clause as OperatorExpression;
    expect(op.type).toBe(ExpressionType.OPERATOR_IN);
    expect(op.children).toHaveLength(2); // id + 1 value
  });

  it('parses IN with string values', () => {
    const node = selectNode("SELECT * FROM t WHERE name IN ('alice', 'bob')");
    const op = node.where_clause as OperatorExpression;
    expect(op.type).toBe(ExpressionType.OPERATOR_IN);
    expect(op.children).toHaveLength(3);
  });

  it('parses BETWEEN with column expressions', () => {
    const node = selectNode('SELECT * FROM t WHERE x BETWEEN low AND high');
    const between = node.where_clause as BetweenExpression;
    expect((between.input as ColumnRefExpression).column_names).toEqual(['x']);
    expect((between.lower as ColumnRefExpression).column_names).toEqual(['low']);
    expect((between.upper as ColumnRefExpression).column_names).toEqual(['high']);
  });

  it('parses mixed AND/OR with parentheses overriding precedence', () => {
    const node = selectNode('SELECT * FROM t WHERE a = 1 AND (b = 2 OR c = 3)');
    const and = node.where_clause as ConjunctionExpression;
    expect(and.type).toBe(ExpressionType.CONJUNCTION_AND);
    expect(and.children).toHaveLength(2);
    const or = and.children[1] as ConjunctionExpression;
    expect(or.type).toBe(ExpressionType.CONJUNCTION_OR);
  });

  it('parses NOT with parenthesized expression', () => {
    const node = selectNode('SELECT * FROM t WHERE NOT (a = 1 OR b = 2)');
    const notExpr = node.where_clause as OperatorExpression;
    expect(notExpr.type).toBe(ExpressionType.OPERATOR_NOT);
    const or = notExpr.children[0] as ConjunctionExpression;
    expect(or.type).toBe(ExpressionType.CONJUNCTION_OR);
  });

  it('parses complex WHERE: NOT IN + AND + OR', () => {
    const node = selectNode("SELECT * FROM t WHERE (a NOT IN (1, 2) AND b > 0) OR c IS NULL");
    const or = node.where_clause as ConjunctionExpression;
    expect(or.type).toBe(ExpressionType.CONJUNCTION_OR);
  });
});

// ============================================================================
// Edge cases: Expressions (arithmetic, functions, CASE, CAST)
// ============================================================================

describe('Expression edge cases', () => {
  it('parses subtraction', () => {
    const node = selectNode('SELECT a - b FROM t');
    const sub = node.select_list[0] as OperatorExpression;
    expect(sub.type).toBe(ExpressionType.OPERATOR_SUBTRACT);
  });

  it('parses division', () => {
    const node = selectNode('SELECT a / b FROM t');
    const div = node.select_list[0] as OperatorExpression;
    expect(div.type).toBe(ExpressionType.OPERATOR_DIVIDE);
  });

  it('parses modulo', () => {
    const node = selectNode('SELECT a % b FROM t');
    const mod = node.select_list[0] as OperatorExpression;
    expect(mod.type).toBe(ExpressionType.OPERATOR_MOD);
  });

  it('parses arithmetic with parentheses overriding precedence', () => {
    const node = selectNode('SELECT (a + b) * c FROM t');
    const mul = node.select_list[0] as OperatorExpression;
    expect(mul.type).toBe(ExpressionType.OPERATOR_MULTIPLY);
    const add = mul.children[0] as OperatorExpression;
    expect(add.type).toBe(ExpressionType.OPERATOR_ADD);
  });

  it('parses chained arithmetic left-to-right', () => {
    const node = selectNode('SELECT a + b + c FROM t');
    // (a + b) + c — left-associative
    const outer = node.select_list[0] as OperatorExpression;
    expect(outer.type).toBe(ExpressionType.OPERATOR_ADD);
    const inner = outer.children[0] as OperatorExpression;
    expect(inner.type).toBe(ExpressionType.OPERATOR_ADD);
    expect((inner.children[0] as ColumnRefExpression).column_names).toEqual(['a']);
    expect((inner.children[1] as ColumnRefExpression).column_names).toEqual(['b']);
    expect((outer.children[1] as ColumnRefExpression).column_names).toEqual(['c']);
  });

  it('parses mixed mul/div left-to-right', () => {
    const node = selectNode('SELECT a * b / c FROM t');
    const div = node.select_list[0] as OperatorExpression;
    expect(div.type).toBe(ExpressionType.OPERATOR_DIVIDE);
    const mul = div.children[0] as OperatorExpression;
    expect(mul.type).toBe(ExpressionType.OPERATOR_MULTIPLY);
  });

  it('parses unary plus (ignored)', () => {
    const node = selectNode('SELECT +1');
    const val = node.select_list[0] as ConstantExpression;
    expect(val.value.value).toBe(1);
  });

  it('parses double unary minus', () => {
    const node = selectNode('SELECT - -1');
    const outer = node.select_list[0] as OperatorExpression;
    expect(outer.type).toBe(ExpressionType.OPERATOR_NEGATE);
    const inner = outer.children[0] as OperatorExpression;
    expect(inner.type).toBe(ExpressionType.OPERATOR_NEGATE);
  });

  it('parses negative in arithmetic', () => {
    const node = selectNode('SELECT a + -b FROM t');
    const add = node.select_list[0] as OperatorExpression;
    expect(add.type).toBe(ExpressionType.OPERATOR_ADD);
    const neg = add.children[1] as OperatorExpression;
    expect(neg.type).toBe(ExpressionType.OPERATOR_NEGATE);
  });

  it('parses function with multiple arguments', () => {
    const node = selectNode('SELECT COALESCE(a, b, c) FROM t');
    const func = node.select_list[0] as FunctionExpression;
    expect(func.function_name).toBe('coalesce');
    expect(func.children).toHaveLength(3);
  });

  it('parses function with no arguments', () => {
    const node = selectNode('SELECT NOW()');
    const func = node.select_list[0] as FunctionExpression;
    expect(func.function_name).toBe('now');
    expect(func.children).toHaveLength(0);
    expect(func.is_star).toBe(false);
  });

  it('parses nested function calls', () => {
    const node = selectNode('SELECT UPPER(TRIM(name)) FROM t');
    const outer = node.select_list[0] as FunctionExpression;
    expect(outer.function_name).toBe('upper');
    expect(outer.children).toHaveLength(1);
    const inner = outer.children[0] as FunctionExpression;
    expect(inner.function_name).toBe('trim');
  });

  it('parses function name case-insensitively', () => {
    const node = selectNode('SELECT Count(*) FROM t');
    const func = node.select_list[0] as FunctionExpression;
    expect(func.function_name).toBe('count');
    expect(func.is_star).toBe(true);
  });

  it('parses CASE without ELSE', () => {
    const node = selectNode("SELECT CASE WHEN a = 1 THEN 'one' END FROM t");
    const caseExpr = node.select_list[0] as CaseExpression;
    expect(caseExpr.case_checks).toHaveLength(1);
    expect(caseExpr.else_expr).toBeNull();
  });

  it('parses CASE with multiple WHEN clauses', () => {
    const node = selectNode("SELECT CASE WHEN a = 1 THEN 'one' WHEN a = 2 THEN 'two' WHEN a = 3 THEN 'three' ELSE 'other' END FROM t");
    const caseExpr = node.select_list[0] as CaseExpression;
    expect(caseExpr.case_checks).toHaveLength(3);
    expect(caseExpr.else_expr).not.toBeNull();
  });

  it('parses CAST to INTEGER', () => {
    const node = selectNode("SELECT CAST('123' AS INTEGER)");
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.INTEGER);
  });

  it('parses CAST to BOOLEAN', () => {
    const node = selectNode('SELECT CAST(1 AS BOOLEAN)');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.BOOLEAN);
  });

  it('parses CAST to BLOB', () => {
    const node = selectNode("SELECT CAST(data AS BLOB) FROM t");
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.BLOB);
  });

  it('parses CAST to BIGINT', () => {
    const node = selectNode('SELECT CAST(id AS BIGINT) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.BIGINT);
  });

  it('parses CAST to DOUBLE', () => {
    const node = selectNode('SELECT CAST(x AS DOUBLE) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.DOUBLE);
  });

  it('parses CAST to FLOAT (REAL maps to FLOAT)', () => {
    const node = selectNode('SELECT CAST(x AS REAL) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.FLOAT);
  });

  it('parses CAST to SMALLINT', () => {
    const node = selectNode('SELECT CAST(x AS SMALLINT) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.SMALLINT);
  });

  it('parses VARCHAR(n) in CAST (size is consumed)', () => {
    const node = selectNode('SELECT CAST(x AS VARCHAR(255)) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.VARCHAR);
  });

  it('parses CHAR(n) in CAST (maps to VARCHAR)', () => {
    const node = selectNode('SELECT CAST(x AS CHAR(10)) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.VARCHAR);
  });

  it('parses scalar subquery in expression', () => {
    const node = selectNode('SELECT a + (SELECT MAX(b) FROM t2) FROM t');
    const add = node.select_list[0] as OperatorExpression;
    expect(add.type).toBe(ExpressionType.OPERATOR_ADD);
    const sub = add.children[1] as SubqueryExpression;
    expect(sub.subquery_type).toBe('SCALAR');
  });

  it('parses expression with alias', () => {
    const node = selectNode('SELECT a + b AS total FROM t');
    const expr = node.select_list[0];
    expect(expr.alias).toBe('total');
    expect(expr.expression_class).toBe(ExpressionClass.OPERATOR);
  });
});

// ============================================================================
// Edge cases: JOIN
// ============================================================================

describe('JOIN edge cases', () => {
  it('parses LEFT OUTER JOIN', () => {
    const node = selectNode('SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.LEFT);
  });

  it('parses RIGHT OUTER JOIN', () => {
    const node = selectNode('SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.RIGHT);
  });

  it('parses multiple chained JOINs (3 tables)', () => {
    const node = selectNode('SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id');
    // The result should be: JOIN(JOIN(a, b), c)
    const outerJoin = node.from_table as JoinRef;
    expect(outerJoin.type).toBe(TableRefType.JOIN);
    expect((outerJoin.right as BaseTableRef).table_name).toBe('c');
    const innerJoin = outerJoin.left as JoinRef;
    expect(innerJoin.type).toBe(TableRefType.JOIN);
    expect((innerJoin.left as BaseTableRef).table_name).toBe('a');
    expect((innerJoin.right as BaseTableRef).table_name).toBe('b');
  });

  it('parses 4 chained JOINs', () => {
    const node = selectNode('SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id CROSS JOIN d');
    const j3 = node.from_table as JoinRef;
    expect(j3.join_type).toBe(JoinType.CROSS);
    expect((j3.right as BaseTableRef).table_name).toBe('d');
    const j2 = j3.left as JoinRef;
    expect(j2.join_type).toBe(JoinType.LEFT);
    const j1 = j2.left as JoinRef;
    expect(j1.join_type).toBe(JoinType.INNER);
  });

  it('parses USING with multiple columns', () => {
    const node = selectNode('SELECT * FROM a JOIN b USING (id, name, type)');
    const join = node.from_table as JoinRef;
    expect(join.using_columns).toEqual(['id', 'name', 'type']);
  });

  it('parses JOIN with subquery as right side', () => {
    const node = selectNode('SELECT * FROM a JOIN (SELECT * FROM b) AS sub ON a.id = sub.id');
    const join = node.from_table as JoinRef;
    expect(join.right.type).toBe(TableRefType.SUBQUERY);
    expect((join.right as SubqueryRef).alias).toBe('sub');
  });

  it('parses CROSS JOIN with aliases', () => {
    const node = selectNode('SELECT * FROM a AS x CROSS JOIN b AS y');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.CROSS);
    expect((join.left as BaseTableRef).alias).toBe('x');
    expect((join.right as BaseTableRef).alias).toBe('y');
  });

  it('parses JOIN with complex ON condition', () => {
    const node = selectNode('SELECT * FROM a JOIN b ON a.id = b.a_id AND a.type = b.type');
    const join = node.from_table as JoinRef;
    const cond = join.condition as ConjunctionExpression;
    expect(cond.type).toBe(ExpressionType.CONJUNCTION_AND);
    expect(cond.children).toHaveLength(2);
  });
});

// ============================================================================
// Edge cases: GROUP BY / HAVING
// ============================================================================

describe('GROUP BY / HAVING edge cases', () => {
  it('parses GROUP BY multiple columns', () => {
    const node = selectNode('SELECT dept, role, COUNT(*) FROM emp GROUP BY dept, role');
    expect(node.groups.group_expressions).toHaveLength(2);
    expect((node.groups.group_expressions[0] as ColumnRefExpression).column_names).toEqual(['dept']);
    expect((node.groups.group_expressions[1] as ColumnRefExpression).column_names).toEqual(['role']);
  });

  it('parses GROUP BY without HAVING', () => {
    const node = selectNode('SELECT dept, COUNT(*) FROM emp GROUP BY dept');
    expect(node.groups.group_expressions).toHaveLength(1);
    expect(node.having).toBeNull();
  });

  it('parses HAVING with complex expression', () => {
    const node = selectNode('SELECT dept FROM emp GROUP BY dept HAVING COUNT(*) > 5 AND SUM(salary) > 100000');
    const conj = node.having as ConjunctionExpression;
    expect(conj.type).toBe(ExpressionType.CONJUNCTION_AND);
    expect(conj.children).toHaveLength(2);
  });

  it('parses GROUP BY with expression', () => {
    const node = selectNode('SELECT a + b, COUNT(*) FROM t GROUP BY a + b');
    const groupExpr = node.groups.group_expressions[0] as OperatorExpression;
    expect(groupExpr.type).toBe(ExpressionType.OPERATOR_ADD);
  });
});

// ============================================================================
// Edge cases: ORDER BY / LIMIT / OFFSET / DISTINCT
// ============================================================================

describe('ORDER BY / LIMIT / OFFSET / DISTINCT edge cases', () => {
  it('parses ORDER BY without ASC/DESC (defaults to ASC)', () => {
    const node = selectNode('SELECT * FROM t ORDER BY name');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].type).toBe(OrderType.ASCENDING);
  });

  it('defaults NULLS LAST for ASC', () => {
    const node = selectNode('SELECT * FROM t ORDER BY name ASC');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].null_order).toBe(OrderByNullType.NULLS_LAST);
  });

  it('defaults NULLS FIRST for DESC', () => {
    const node = selectNode('SELECT * FROM t ORDER BY name DESC');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].null_order).toBe(OrderByNullType.NULLS_FIRST);
  });

  it('parses ORDER BY DESC NULLS LAST (override default)', () => {
    const node = selectNode('SELECT * FROM t ORDER BY name DESC NULLS LAST');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].type).toBe(OrderType.DESCENDING);
    expect(orderMod.orders[0].null_order).toBe(OrderByNullType.NULLS_LAST);
  });

  it('parses ORDER BY ASC NULLS FIRST (override default)', () => {
    const node = selectNode('SELECT * FROM t ORDER BY name ASC NULLS FIRST');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].type).toBe(OrderType.ASCENDING);
    expect(orderMod.orders[0].null_order).toBe(OrderByNullType.NULLS_FIRST);
  });

  it('parses LIMIT without OFFSET', () => {
    const node = selectNode('SELECT * FROM t LIMIT 5');
    const limitMod = node.modifiers.find(m => m.type === ResultModifierType.LIMIT_MODIFIER) as LimitModifier;
    expect((limitMod.limit as ConstantExpression).value.value).toBe(5);
    expect(limitMod.offset).toBeNull();
  });

  it('parses OFFSET before LIMIT', () => {
    const node = selectNode('SELECT * FROM t OFFSET 10 LIMIT 5');
    const limitMod = node.modifiers.find(m => m.type === ResultModifierType.LIMIT_MODIFIER) as LimitModifier;
    expect((limitMod.limit as ConstantExpression).value.value).toBe(5);
    expect((limitMod.offset as ConstantExpression).value.value).toBe(10);
  });

  it('parses ORDER BY with expression', () => {
    const node = selectNode('SELECT * FROM t ORDER BY a + b DESC');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].type).toBe(OrderType.DESCENDING);
    expect((orderMod.orders[0].expression as OperatorExpression).type).toBe(ExpressionType.OPERATOR_ADD);
  });

  it('parses ORDER BY with many columns', () => {
    const node = selectNode('SELECT * FROM t ORDER BY a ASC, b DESC, c ASC NULLS FIRST');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders).toHaveLength(3);
    expect(orderMod.orders[0].type).toBe(OrderType.ASCENDING);
    expect(orderMod.orders[1].type).toBe(OrderType.DESCENDING);
    expect(orderMod.orders[2].null_order).toBe(OrderByNullType.NULLS_FIRST);
  });

  it('parses DISTINCT with specific columns', () => {
    const node = selectNode('SELECT DISTINCT name, email FROM users');
    const distinctMod = node.modifiers.find(m => m.type === ResultModifierType.DISTINCT_MODIFIER) as DistinctModifier;
    expect(distinctMod).toBeDefined();
    expect(node.select_list).toHaveLength(2);
  });
});

// ============================================================================
// Edge cases: CTE
// ============================================================================

describe('CTE edge cases', () => {
  it('parses multiple CTEs', () => {
    const stmt = parseSelect(`
      WITH
        cte1 AS (SELECT 1 AS a),
        cte2 AS (SELECT 2 AS b)
      SELECT * FROM cte1 JOIN cte2 ON cte1.a = cte2.b
    `);
    const node = stmt.node as SelectNode;
    expect(node.cte_map.map['cte1']).toBeDefined();
    expect(node.cte_map.map['cte2']).toBeDefined();
  });

  it('parses CTE with column aliases', () => {
    const stmt = parseSelect('WITH cte(x, y) AS (SELECT 1, 2) SELECT * FROM cte');
    const node = stmt.node as SelectNode;
    expect(node.cte_map.map['cte'].aliases).toEqual(['x', 'y']);
  });

  it('parses CTE with complex inner query', () => {
    const stmt = parseSelect(`
      WITH ranked AS (
        SELECT id, name, COUNT(*) AS cnt
        FROM users
        GROUP BY id, name
        HAVING COUNT(*) > 1
      )
      SELECT * FROM ranked WHERE cnt > 5 ORDER BY cnt DESC
    `);
    const node = stmt.node as SelectNode;
    expect(node.cte_map.map['ranked']).toBeDefined();
    expect(node.where_clause).not.toBeNull();
  });
});

// ============================================================================
// Edge cases: Subquery
// ============================================================================

describe('Subquery edge cases', () => {
  it('parses subquery in FROM without AS keyword', () => {
    const node = selectNode('SELECT * FROM (SELECT * FROM users) sub');
    expect(node.from_table!.type).toBe(TableRefType.SUBQUERY);
    expect((node.from_table as SubqueryRef).alias).toBe('sub');
  });

  it('parses nested subqueries in FROM', () => {
    const node = selectNode('SELECT * FROM (SELECT * FROM (SELECT 1 AS x) AS inner_sub) AS outer_sub');
    expect(node.from_table!.type).toBe(TableRefType.SUBQUERY);
    const outer = node.from_table as SubqueryRef;
    expect(outer.alias).toBe('outer_sub');
    const innerSelect = (outer.subquery.node as SelectNode);
    expect(innerSelect.from_table!.type).toBe(TableRefType.SUBQUERY);
  });

  it('parses EXISTS with correlated subquery', () => {
    const node = selectNode('SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)');
    const sub = node.where_clause as SubqueryExpression;
    expect(sub.subquery_type).toBe('EXISTS');
    const innerNode = sub.subquery.node as SelectNode;
    expect(innerNode.where_clause).not.toBeNull();
  });

  it('parses IN subquery with WHERE clause in subquery', () => {
    const node = selectNode("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)");
    const sub = node.where_clause as SubqueryExpression;
    expect(sub.subquery_type).toBe('ANY');
    const innerNode = sub.subquery.node as SelectNode;
    expect(innerNode.where_clause).not.toBeNull();
  });

  it('parses scalar subquery in WHERE comparison', () => {
    const node = selectNode('SELECT * FROM t WHERE a > (SELECT MAX(b) FROM t2)');
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_GREATERTHAN);
    const sub = cmp.right as SubqueryExpression;
    expect(sub.subquery_type).toBe('SCALAR');
  });
});

// ============================================================================
// Edge cases: UNION
// ============================================================================

describe('UNION edge cases', () => {
  it('parses UNION with ORDER BY and LIMIT', () => {
    const stmt = parseOne('SELECT 1 UNION ALL SELECT 2 ORDER BY 1 LIMIT 1');
    const setStmt = stmt as SetOperationStatement;
    expect(setStmt.node.modifiers).toHaveLength(2);
    const orderMod = setStmt.node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod).toBeDefined();
    const limitMod = setStmt.node.modifiers.find(m => m.type === ResultModifierType.LIMIT_MODIFIER) as LimitModifier;
    expect(limitMod).toBeDefined();
  });

  it('parses UNION with complex queries on both sides', () => {
    const stmt = parseOne('SELECT id, name FROM users WHERE active = 1 UNION SELECT id, name FROM admins WHERE role = 1');
    const setStmt = stmt as SetOperationStatement;
    expect(setStmt.node.type).toBe('SET_OPERATION_NODE');
    const left = setStmt.node.left as SelectNode;
    expect(left.where_clause).not.toBeNull();
    const right = setStmt.node.right as SelectNode;
    expect(right.where_clause).not.toBeNull();
  });

  it('parses 3-way UNION ALL', () => {
    const stmt = parseOne('SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3');
    const setStmt = stmt as SetOperationStatement;
    expect(setStmt.node.set_op_type).toBe(SetOperationType.UNION_ALL);
    expect(setStmt.node.left.type).toBe('SET_OPERATION_NODE');
    const inner = setStmt.node.left as SetOperationNode;
    expect(inner.set_op_type).toBe(SetOperationType.UNION_ALL);
  });

  it('parses mixed UNION and UNION ALL', () => {
    const stmt = parseOne('SELECT 1 UNION SELECT 2 UNION ALL SELECT 3');
    const setStmt = stmt as SetOperationStatement;
    expect(setStmt.node.set_op_type).toBe(SetOperationType.UNION_ALL);
    const inner = setStmt.node.left as SetOperationNode;
    expect(inner.set_op_type).toBe(SetOperationType.UNION);
  });
});

// ============================================================================
// Edge cases: DML
// ============================================================================

describe('DML edge cases', () => {
  it('parses INSERT without column list', () => {
    const stmt = parseOne("INSERT INTO users VALUES ('Ivan', 25)") as InsertStatement;
    expect(stmt.columns).toEqual([]);
    expect(stmt.values).toHaveLength(1);
    expect(stmt.values[0]).toHaveLength(2);
  });

  it('parses INSERT with expressions in values', () => {
    const stmt = parseOne("INSERT INTO t (a, b) VALUES (1 + 2, 'hello')") as InsertStatement;
    const addExpr = stmt.values[0][0] as OperatorExpression;
    expect(addExpr.type).toBe(ExpressionType.OPERATOR_ADD);
  });

  it('parses INSERT with many rows', () => {
    const stmt = parseOne("INSERT INTO t VALUES (1), (2), (3), (4), (5)") as InsertStatement;
    expect(stmt.values).toHaveLength(5);
  });

  it('parses INSERT ... SELECT with WHERE clause', () => {
    const stmt = parseOne("INSERT INTO archive (id, name) SELECT id, name FROM users WHERE active = 0") as InsertStatement;
    expect(stmt.columns).toEqual(['id', 'name']);
    expect(stmt.select_statement).not.toBeNull();
    const innerNode = stmt.select_statement!.node as SelectNode;
    expect(innerNode.where_clause).not.toBeNull();
  });

  it('parses UPDATE without WHERE', () => {
    const stmt = parseOne("UPDATE t SET active = FALSE") as UpdateStatement;
    expect(stmt.where_clause).toBeNull();
    expect(stmt.set_clauses).toHaveLength(1);
  });

  it('parses UPDATE with complex SET value', () => {
    const stmt = parseOne("UPDATE t SET total = price * quantity WHERE id = 1") as UpdateStatement;
    const mul = stmt.set_clauses[0].value as OperatorExpression;
    expect(mul.type).toBe(ExpressionType.OPERATOR_MULTIPLY);
  });

  it('parses DELETE with complex WHERE', () => {
    const stmt = parseOne("DELETE FROM t WHERE a > 10 AND (b = 1 OR c = 2)") as DeleteStatement;
    const and = stmt.where_clause as ConjunctionExpression;
    expect(and.type).toBe(ExpressionType.CONJUNCTION_AND);
    expect(and.children).toHaveLength(2);
  });

  it('parses INSERT with NULL and boolean values', () => {
    const stmt = parseOne("INSERT INTO t (a, b, c) VALUES (NULL, TRUE, FALSE)") as InsertStatement;
    const row = stmt.values[0];
    expect((row[0] as ConstantExpression).value.is_null).toBe(true);
    expect((row[1] as ConstantExpression).value.value).toBe(true);
    expect((row[2] as ConstantExpression).value.value).toBe(false);
  });
});

// ============================================================================
// Edge cases: DDL
// ============================================================================

describe('DDL edge cases', () => {
  it('parses CREATE TABLE with all column types', () => {
    const stmt = parseOne(`
      CREATE TABLE t (
        a INTEGER,
        b INT,
        c BIGINT,
        d SMALLINT,
        e REAL,
        f FLOAT,
        g DOUBLE,
        h TEXT,
        i VARCHAR,
        j CHAR,
        k BLOB,
        l BOOLEAN,
        m BOOL
      )
    `) as CreateTableStatement;
    expect(stmt.columns).toHaveLength(13);
    expect(stmt.columns[0].type.id).toBe(LogicalTypeId.INTEGER);
    expect(stmt.columns[1].type.id).toBe(LogicalTypeId.INTEGER);
    expect(stmt.columns[2].type.id).toBe(LogicalTypeId.BIGINT);
    expect(stmt.columns[3].type.id).toBe(LogicalTypeId.SMALLINT);
    expect(stmt.columns[4].type.id).toBe(LogicalTypeId.FLOAT);
    expect(stmt.columns[5].type.id).toBe(LogicalTypeId.FLOAT);
    expect(stmt.columns[6].type.id).toBe(LogicalTypeId.DOUBLE);
    expect(stmt.columns[7].type.id).toBe(LogicalTypeId.VARCHAR);
    expect(stmt.columns[8].type.id).toBe(LogicalTypeId.VARCHAR);
    expect(stmt.columns[9].type.id).toBe(LogicalTypeId.VARCHAR);
    expect(stmt.columns[10].type.id).toBe(LogicalTypeId.BLOB);
    expect(stmt.columns[11].type.id).toBe(LogicalTypeId.BOOLEAN);
    expect(stmt.columns[12].type.id).toBe(LogicalTypeId.BOOLEAN);
  });

  it('parses CREATE TABLE with VARCHAR(n)', () => {
    const stmt = parseOne('CREATE TABLE t (name VARCHAR(255))') as CreateTableStatement;
    expect(stmt.columns[0].type.id).toBe(LogicalTypeId.VARCHAR);
  });

  it('parses CREATE TABLE with multiple constraints on one column', () => {
    const stmt = parseOne("CREATE TABLE t (id INTEGER PRIMARY KEY NOT NULL UNIQUE DEFAULT 0)") as CreateTableStatement;
    const col = stmt.columns[0];
    expect(col.is_primary_key).toBe(true);
    expect(col.is_not_null).toBe(true);
    expect(col.is_unique).toBe(true);
    expect(col.default_value).not.toBeNull();
  });

  it('parses CREATE TABLE with compound FOREIGN KEY', () => {
    const stmt = parseOne(`
      CREATE TABLE t (
        a INTEGER,
        b INTEGER,
        FOREIGN KEY (a, b) REFERENCES other(x, y)
      )
    `) as CreateTableStatement;
    expect(stmt.foreign_keys).toHaveLength(1);
    expect(stmt.foreign_keys[0].columns).toEqual(['a', 'b']);
    expect(stmt.foreign_keys[0].ref_columns).toEqual(['x', 'y']);
  });

  it('parses CREATE TABLE with DEFAULT string value', () => {
    const stmt = parseOne("CREATE TABLE t (status TEXT DEFAULT 'active')") as CreateTableStatement;
    const def = stmt.columns[0].default_value as ConstantExpression;
    expect(def.value.value).toBe('active');
  });

  it('parses CREATE TABLE with DEFAULT NULL', () => {
    const stmt = parseOne("CREATE TABLE t (notes TEXT DEFAULT NULL)") as CreateTableStatement;
    const def = stmt.columns[0].default_value as ConstantExpression;
    expect(def.value.is_null).toBe(true);
  });

  it('parses CREATE TABLE with DEFAULT FALSE', () => {
    const stmt = parseOne("CREATE TABLE t (active BOOLEAN DEFAULT FALSE)") as CreateTableStatement;
    const def = stmt.columns[0].default_value as ConstantExpression;
    expect(def.value.value).toBe(false);
  });

  it('parses CREATE INDEX with multiple columns', () => {
    const stmt = parseOne('CREATE INDEX idx ON t (a, b, c)') as CreateIndexStatement;
    expect(stmt.columns).toEqual(['a', 'b', 'c']);
  });

  it('parses CREATE UNIQUE INDEX IF NOT EXISTS', () => {
    const stmt = parseOne('CREATE UNIQUE INDEX IF NOT EXISTS idx ON t (a)') as CreateIndexStatement;
    expect(stmt.is_unique).toBe(true);
    expect(stmt.if_not_exists).toBe(true);
  });

  it('parses ALTER TABLE ADD without COLUMN keyword', () => {
    const stmt = parseOne('ALTER TABLE t ADD email TEXT') as AlterTableStatement;
    expect(stmt.alter_type).toBe(AlterType.ADD_COLUMN);
    expect(stmt.column_def!.name).toBe('email');
  });

  it('parses ALTER TABLE DROP without COLUMN keyword', () => {
    const stmt = parseOne('ALTER TABLE t DROP email') as AlterTableStatement;
    expect(stmt.alter_type).toBe(AlterType.DROP_COLUMN);
    expect(stmt.column_name).toBe('email');
  });

  it('parses ALTER TABLE ADD COLUMN with constraints', () => {
    const stmt = parseOne('ALTER TABLE t ADD COLUMN email TEXT NOT NULL UNIQUE') as AlterTableStatement;
    expect(stmt.column_def!.is_not_null).toBe(true);
    expect(stmt.column_def!.is_unique).toBe(true);
  });

  it('parses DROP INDEX IF EXISTS', () => {
    const stmt = parseOne('DROP INDEX IF EXISTS idx_name') as DropStatement;
    expect(stmt.drop_type).toBe(DropType.INDEX);
    expect(stmt.if_exists).toBe(true);
    expect(stmt.name).toBe('idx_name');
  });

  it('parses CREATE TABLE with multiple table-level constraints', () => {
    const stmt = parseOne(`
      CREATE TABLE t (
        a INTEGER,
        b INTEGER,
        c INTEGER,
        PRIMARY KEY (a),
        FOREIGN KEY (b) REFERENCES ref1(id),
        FOREIGN KEY (c) REFERENCES ref2(id)
      )
    `) as CreateTableStatement;
    expect(stmt.primary_key).toEqual(['a']);
    expect(stmt.foreign_keys).toHaveLength(2);
    expect(stmt.foreign_keys[0].ref_table).toBe('ref1');
    expect(stmt.foreign_keys[1].ref_table).toBe('ref2');
  });
});

// ============================================================================
// Edge cases: TCL
// ============================================================================

describe('TCL edge cases', () => {
  it('parses COMMIT TRANSACTION', () => {
    const stmt = parseOne('COMMIT TRANSACTION') as TransactionStatement;
    expect(stmt.transaction_type).toBe(TransactionType.COMMIT);
  });

  it('parses ROLLBACK TRANSACTION', () => {
    const stmt = parseOne('ROLLBACK TRANSACTION') as TransactionStatement;
    expect(stmt.transaction_type).toBe(TransactionType.ROLLBACK);
  });
});

// ============================================================================
// Edge cases: Multiple statements
// ============================================================================

describe('Multiple statements edge cases', () => {
  it('parses different statement types separated by semicolons', () => {
    const stmts = parse('SELECT 1; INSERT INTO t VALUES (1); DELETE FROM t; BEGIN');
    expect(stmts).toHaveLength(4);
    expect(stmts[0].type).toBe(StatementType.SELECT_STATEMENT);
    expect(stmts[1].type).toBe(StatementType.INSERT_STATEMENT);
    expect(stmts[2].type).toBe(StatementType.DELETE_STATEMENT);
    expect(stmts[3].type).toBe(StatementType.TRANSACTION_STATEMENT);
  });

  it('handles multiple empty semicolons between statements', () => {
    const stmts = parse('SELECT 1 ;; ; SELECT 2');
    expect(stmts).toHaveLength(2);
  });

  it('handles only semicolons', () => {
    const stmts = parse(';;;');
    expect(stmts).toHaveLength(0);
  });

  it('handles empty input', () => {
    const stmts = parse('');
    expect(stmts).toHaveLength(0);
  });

  it('handles whitespace-only input', () => {
    const stmts = parse('   \n\t  ');
    expect(stmts).toHaveLength(0);
  });
});

// ============================================================================
// Edge cases: Errors
// ============================================================================

describe('Error edge cases', () => {
  it('throws ParseError for unexpected token at start', () => {
    expect(() => parse('FOO')).toThrow(ParseError);
  });

  it('throws for missing closing parenthesis in expression', () => {
    expect(() => parse('SELECT (1 + 2')).toThrow();
  });

  it('allows JOIN without ON/USING (implicit cross join)', () => {
    const node = selectNode('SELECT * FROM a JOIN b WHERE a.id = 1');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.INNER);
    expect(join.condition).toBeNull();
    expect(join.using_columns).toEqual([]);
    expect(node.where_clause).not.toBeNull();
  });

  it('throws for DROP without TABLE or INDEX', () => {
    expect(() => parse('DROP VIEW foo')).toThrow();
  });

  it('throws for CREATE without TABLE or INDEX', () => {
    expect(() => parse('CREATE VIEW foo AS SELECT 1')).toThrow();
  });

  it('throws for ALTER TABLE without ADD or DROP', () => {
    expect(() => parse('ALTER TABLE t RENAME TO t2')).toThrow();
  });

  it('throws for unsupported OVER keyword', () => {
    expect(() => parse('SELECT OVER')).toThrow(/not supported/);
  });

  it('throws for unsupported PARTITION keyword', () => {
    expect(() => parse('SELECT PARTITION')).toThrow(/not supported/);
  });

  it('throws for unsupported FULL keyword', () => {
    expect(() => parse('SELECT FULL')).toThrow(/not supported/);
  });

  it('throws for unsupported TRUNCATE keyword', () => {
    expect(() => parse('TRUNCATE TABLE t')).toThrow(/not supported/);
  });

  it('throws for missing table name after FROM', () => {
    expect(() => parse('SELECT * FROM')).toThrow();
  });

  it('throws for missing expression after WHERE', () => {
    expect(() => parse('SELECT * FROM t WHERE')).toThrow();
  });

  it('throws for missing VALUES in INSERT', () => {
    expect(() => parse('INSERT INTO t (a)')).toThrow();
  });

  it('throws for missing SET in UPDATE', () => {
    expect(() => parse('UPDATE t WHERE id = 1')).toThrow();
  });

  it('throws for missing FROM in DELETE', () => {
    expect(() => parse('DELETE users')).toThrow();
  });

  it('throws for unterminated string', () => {
    expect(() => parse("SELECT 'hello")).toThrow();
  });

  it('throws for unexpected character', () => {
    expect(() => parse('SELECT @foo')).toThrow();
  });

  it('includes line and column in error', () => {
    try {
      parse('SELECT 1\nSELECT FROM t');
      expect.unreachable('should have thrown');
    } catch (e) {
      expect(e).toBeInstanceOf(ParseError);
      const pe = e as ParseError;
      expect(pe.line).toBe(2);
    }
  });

  it('throws for NULLS without FIRST or LAST', () => {
    expect(() => parse('SELECT * FROM t ORDER BY a NULLS')).toThrow();
  });

  it('throws for missing THEN in CASE', () => {
    expect(() => parse('SELECT CASE WHEN 1 END')).toThrow();
  });

  it('throws for missing END in CASE', () => {
    expect(() => parse("SELECT CASE WHEN 1 THEN 'a'")).toThrow();
  });

  it('throws for missing AS in CAST', () => {
    expect(() => parse('SELECT CAST(1 INTEGER)')).toThrow();
  });

  it('throws for invalid type name in CAST', () => {
    expect(() => parse('SELECT CAST(1 AS INVALID_TYPE)')).toThrow();
  });

  it('throws for SELECT with no expressions before FROM', () => {
    expect(() => parse('SELECT FROM t')).toThrow();
  });

  it('throws for missing INTO after INSERT', () => {
    expect(() => parse('INSERT users VALUES (1)')).toThrow();
  });

  it('throws for unterminated block comment', () => {
    expect(() => parse('SELECT /* unclosed comment 1')).toThrow(/unterminated/i);
  });

  it('throws for unterminated quoted identifier', () => {
    expect(() => parse('SELECT "unclosed')).toThrow(/unterminated/i);
  });

  it('throws for lone ! character', () => {
    expect(() => parse('SELECT !')).toThrow();
  });

  it('throws for missing paren after IN', () => {
    expect(() => parse('SELECT * FROM t WHERE a IN 1, 2')).toThrow();
  });

  it('throws for missing closing paren in IN list', () => {
    expect(() => parse('SELECT * FROM t WHERE a IN (1, 2')).toThrow();
  });

  it('throws for missing closing paren in CAST', () => {
    expect(() => parse('SELECT CAST(1 AS INTEGER')).toThrow();
  });

  it('throws for missing AS after CTE name', () => {
    expect(() => parse('WITH cte (SELECT 1) SELECT * FROM cte')).toThrow();
  });

  it('throws for missing parens around CTE body', () => {
    expect(() => parse('WITH cte AS SELECT 1 SELECT * FROM cte')).toThrow();
  });

  it('throws for missing BY after ORDER', () => {
    expect(() => parse('SELECT * FROM t ORDER name')).toThrow();
  });

  it('throws for missing BY after GROUP', () => {
    expect(() => parse('SELECT * FROM t GROUP name')).toThrow();
  });

  it('throws for missing JOIN after CROSS', () => {
    expect(() => parse('SELECT * FROM a CROSS b')).toThrow();
  });

  it('throws for unsupported GRANT', () => {
    expect(() => parse('GRANT something')).toThrow(/not supported/i);
  });

  it('throws for unsupported REVOKE', () => {
    expect(() => parse('REVOKE something')).toThrow(/not supported/i);
  });

  it('throws for unsupported PROCEDURE', () => {
    expect(() => parse('SELECT PROCEDURE')).toThrow(/not supported/i);
  });

  it('throws for unsupported TRIGGER', () => {
    expect(() => parse('SELECT TRIGGER')).toThrow(/not supported/i);
  });

  it('throws for unsupported PIVOT', () => {
    expect(() => parse('SELECT PIVOT')).toThrow(/not supported/i);
  });

  it('throws for unsupported UNPIVOT', () => {
    expect(() => parse('SELECT UNPIVOT')).toThrow(/not supported/i);
  });

  it('throws for missing column name after table dot', () => {
    expect(() => parse('SELECT t. FROM t')).toThrow();
  });

  it('throws for missing closing paren in function call', () => {
    expect(() => parse('SELECT MAX(a FROM t')).toThrow();
  });
});

// ============================================================================
// Edge cases: Expression combinations
// ============================================================================

describe('Expression combinations', () => {
  it('parses CASE nested inside arithmetic', () => {
    const node = selectNode('SELECT 1 + CASE WHEN a > 0 THEN a ELSE 0 END FROM t');
    const add = node.select_list[0] as OperatorExpression;
    expect(add.type).toBe(ExpressionType.OPERATOR_ADD);
    expect(add.children[1].expression_class).toBe(ExpressionClass.CASE);
  });

  it('parses CASE in WHERE clause', () => {
    const node = selectNode('SELECT * FROM t WHERE CASE WHEN a > 0 THEN TRUE ELSE FALSE END');
    expect(node.where_clause!.expression_class).toBe(ExpressionClass.CASE);
  });

  it('parses nested CASE expressions', () => {
    const node = selectNode("SELECT CASE WHEN a = 1 THEN CASE WHEN b = 1 THEN 'x' ELSE 'y' END ELSE 'z' END FROM t");
    const outer = node.select_list[0] as CaseExpression;
    expect(outer.case_checks).toHaveLength(1);
    const inner = outer.case_checks[0].then_expr as CaseExpression;
    expect(inner.expression_class).toBe(ExpressionClass.CASE);
    expect(inner.case_checks).toHaveLength(1);
    expect((outer.else_expr as ConstantExpression).value.value).toBe('z');
  });

  it('parses CAST in WHERE clause', () => {
    const node = selectNode("SELECT * FROM t WHERE CAST(age AS TEXT) = '25'");
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_EQUAL);
    expect(cmp.left.expression_class).toBe(ExpressionClass.CAST);
  });

  it('parses CAST of an expression', () => {
    const node = selectNode('SELECT CAST(a + b AS DOUBLE) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect((cast.child as OperatorExpression).type).toBe(ExpressionType.OPERATOR_ADD);
    expect(cast.cast_type.id).toBe(LogicalTypeId.DOUBLE);
  });

  it('parses function call in ORDER BY', () => {
    const node = selectNode('SELECT * FROM t ORDER BY UPPER(name) ASC');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    const expr = orderMod.orders[0].expression as FunctionExpression;
    expect(expr.function_name).toBe('upper');
  });

  it('parses function call in WHERE clause', () => {
    const node = selectNode('SELECT * FROM t WHERE LENGTH(name) > 5');
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_GREATERTHAN);
    expect((cmp.left as FunctionExpression).function_name).toBe('length');
  });

  it('parses function call in GROUP BY', () => {
    const node = selectNode('SELECT UPPER(name), COUNT(*) FROM t GROUP BY UPPER(name)');
    const groupExpr = node.groups.group_expressions[0] as FunctionExpression;
    expect(groupExpr.function_name).toBe('upper');
  });

  it('parses CASE in ORDER BY', () => {
    const node = selectNode('SELECT * FROM t ORDER BY CASE WHEN a IS NULL THEN 1 ELSE 0 END ASC');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].expression.expression_class).toBe(ExpressionClass.CASE);
  });

  it('parses arithmetic inside CASE THEN/ELSE', () => {
    const node = selectNode('SELECT CASE WHEN x > 0 THEN x * 2 ELSE x + 1 END FROM t');
    const caseExpr = node.select_list[0] as CaseExpression;
    expect((caseExpr.case_checks[0].then_expr as OperatorExpression).type).toBe(ExpressionType.OPERATOR_MULTIPLY);
    expect((caseExpr.else_expr as OperatorExpression).type).toBe(ExpressionType.OPERATOR_ADD);
  });

  it('parses CAST nested inside CASE', () => {
    const node = selectNode("SELECT CASE WHEN a = 1 THEN CAST(b AS TEXT) ELSE 'default' END FROM t");
    const caseExpr = node.select_list[0] as CaseExpression;
    expect(caseExpr.case_checks[0].then_expr.expression_class).toBe(ExpressionClass.CAST);
  });

  it('parses CASE with ELSE NULL', () => {
    const node = selectNode("SELECT CASE WHEN a = 1 THEN 'yes' ELSE NULL END FROM t");
    const caseExpr = node.select_list[0] as CaseExpression;
    expect((caseExpr.else_expr as ConstantExpression).value.is_null).toBe(true);
  });

  it('parses column-to-column comparison', () => {
    const node = selectNode('SELECT * FROM t WHERE a = b');
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_EQUAL);
    expect(cmp.left.expression_class).toBe(ExpressionClass.COLUMN_REF);
    expect(cmp.right.expression_class).toBe(ExpressionClass.COLUMN_REF);
  });

  it('parses qualified column-to-column comparison', () => {
    const node = selectNode('SELECT * FROM a JOIN b ON a.id = b.id WHERE a.name = b.name');
    const cmp = node.where_clause as ComparisonExpression;
    expect((cmp.left as ColumnRefExpression).column_names).toEqual(['a', 'name']);
    expect((cmp.right as ColumnRefExpression).column_names).toEqual(['b', 'name']);
  });

  it('parses float in WHERE', () => {
    const node = selectNode('SELECT * FROM t WHERE price > 9.99');
    const cmp = node.where_clause as ComparisonExpression;
    const right = cmp.right as ConstantExpression;
    expect(right.value.value).toBe(9.99);
    expect(right.value.type.id).toBe(LogicalTypeId.DOUBLE);
  });

  it('parses string comparison in WHERE', () => {
    const node = selectNode("SELECT * FROM t WHERE name = 'alice'");
    const cmp = node.where_clause as ComparisonExpression;
    expect((cmp.right as ConstantExpression).value.type.id).toBe(LogicalTypeId.VARCHAR);
  });

  it('parses boolean constant in WHERE', () => {
    const node = selectNode('SELECT * FROM t WHERE active = TRUE');
    const cmp = node.where_clause as ComparisonExpression;
    expect((cmp.right as ConstantExpression).value.value).toBe(true);
    expect((cmp.right as ConstantExpression).value.type.id).toBe(LogicalTypeId.BOOLEAN);
  });

  it('parses NULL in comparison (not IS NULL)', () => {
    const node = selectNode('SELECT * FROM t WHERE a = NULL');
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_EQUAL);
    expect((cmp.right as ConstantExpression).value.is_null).toBe(true);
  });

  it('parses LIKE with column reference as pattern', () => {
    const node = selectNode('SELECT * FROM t WHERE name LIKE pattern');
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_LIKE);
    expect(cmp.right.expression_class).toBe(ExpressionClass.COLUMN_REF);
  });

  it('parses BETWEEN with arithmetic bounds', () => {
    const node = selectNode('SELECT * FROM t WHERE x BETWEEN a + 1 AND b * 2');
    const between = node.where_clause as BetweenExpression;
    expect((between.lower as OperatorExpression).type).toBe(ExpressionType.OPERATOR_ADD);
    expect((between.upper as OperatorExpression).type).toBe(ExpressionType.OPERATOR_MULTIPLY);
  });

  it('parses BETWEEN with arithmetic input', () => {
    const node = selectNode('SELECT * FROM t WHERE a + b BETWEEN 1 AND 10');
    const between = node.where_clause as BetweenExpression;
    expect((between.input as OperatorExpression).type).toBe(ExpressionType.OPERATOR_ADD);
  });

  it('parses IS NULL on expression', () => {
    const node = selectNode('SELECT * FROM t WHERE a + b IS NULL');
    const op = node.where_clause as OperatorExpression;
    expect(op.type).toBe(ExpressionType.OPERATOR_IS_NULL);
    expect((op.children[0] as OperatorExpression).type).toBe(ExpressionType.OPERATOR_ADD);
  });

  it('parses IS NOT NULL on function result', () => {
    const node = selectNode('SELECT * FROM t WHERE COALESCE(a, b) IS NOT NULL');
    const op = node.where_clause as OperatorExpression;
    expect(op.type).toBe(ExpressionType.OPERATOR_IS_NOT_NULL);
    expect((op.children[0] as FunctionExpression).function_name).toBe('coalesce');
  });

  it('parses unary minus on column reference', () => {
    const node = selectNode('SELECT -a FROM t');
    const neg = node.select_list[0] as OperatorExpression;
    expect(neg.type).toBe(ExpressionType.OPERATOR_NEGATE);
    expect((neg.children[0] as ColumnRefExpression).column_names).toEqual(['a']);
  });

  it('parses COUNT(*) with alias', () => {
    const node = selectNode('SELECT COUNT(*) AS total FROM t');
    const func = node.select_list[0] as FunctionExpression;
    expect(func.is_star).toBe(true);
    expect(func.alias).toBe('total');
  });

  it('parses function with DISTINCT in HAVING', () => {
    const node = selectNode('SELECT dept FROM t GROUP BY dept HAVING COUNT(DISTINCT name) > 3');
    const cmp = node.having as ComparisonExpression;
    const func = cmp.left as FunctionExpression;
    expect(func.distinct).toBe(true);
  });
});

// ============================================================================
// Edge cases: Boundary conditions
// ============================================================================

describe('Boundary conditions', () => {
  it('parses empty string literal', () => {
    const node = selectNode("SELECT ''");
    const val = node.select_list[0] as ConstantExpression;
    expect(val.value.value).toBe('');
    expect(val.value.type.id).toBe(LogicalTypeId.VARCHAR);
  });

  it('parses integer literal 0', () => {
    const node = selectNode('SELECT 0');
    const val = node.select_list[0] as ConstantExpression;
    expect(val.value.value).toBe(0);
  });

  it('parses large integer literal', () => {
    const node = selectNode('SELECT 2147483647');
    const val = node.select_list[0] as ConstantExpression;
    expect(val.value.value).toBe(2147483647);
  });

  it('parses float with leading zero', () => {
    const node = selectNode('SELECT 0.5');
    const val = node.select_list[0] as ConstantExpression;
    expect(val.value.value).toBe(0.5);
    expect(val.value.type.id).toBe(LogicalTypeId.DOUBLE);
  });

  it('parses multiple escaped quotes in string', () => {
    const node = selectNode("SELECT 'a''b''c'");
    const val = node.select_list[0] as ConstantExpression;
    expect(val.value.value).toBe("a'b'c");
  });

  it('parses subquery in FROM without alias', () => {
    const node = selectNode('SELECT * FROM (SELECT 1 AS x)');
    expect(node.from_table!.type).toBe(TableRefType.SUBQUERY);
    expect((node.from_table as SubqueryRef).alias).toBeNull();
  });

  it('parses OFFSET only (no LIMIT)', () => {
    const node = selectNode('SELECT * FROM t OFFSET 5');
    const limitMod = node.modifiers.find(m => m.type === ResultModifierType.LIMIT_MODIFIER) as LimitModifier;
    expect(limitMod.limit).toBeNull();
    expect((limitMod.offset as ConstantExpression).value.value).toBe(5);
  });

  it('parses simple CASE with single WHEN, no ELSE', () => {
    const node = selectNode("SELECT CASE x WHEN 1 THEN 'one' END FROM t");
    const caseExpr = node.select_list[0] as CaseExpression;
    expect(caseExpr.case_checks).toHaveLength(1);
    const check = caseExpr.case_checks[0].when_expr as ComparisonExpression;
    expect(check.type).toBe(ExpressionType.COMPARE_EQUAL);
    expect(caseExpr.else_expr).toBeNull();
  });

  it('parses quoted identifier standalone', () => {
    const node = selectNode('SELECT "my column" FROM t');
    const col = node.select_list[0] as ColumnRefExpression;
    expect(col.column_names).toEqual(['my column']);
  });

  it('parses block comment between tokens', () => {
    const node = selectNode('SELECT /* comment */ 1 /* another */ + /* yet another */ 2');
    const add = node.select_list[0] as OperatorExpression;
    expect(add.type).toBe(ExpressionType.OPERATOR_ADD);
  });

  it('parses empty block comment', () => {
    const node = selectNode('SELECT /**/ 1');
    const val = node.select_list[0] as ConstantExpression;
    expect(val.value.value).toBe(1);
  });

  it('parses identifier with underscore prefix', () => {
    const node = selectNode('SELECT _private FROM t');
    const col = node.select_list[0] as ColumnRefExpression;
    expect(col.column_names).toEqual(['_private']);
  });

  it('parses identifier with digits', () => {
    const node = selectNode('SELECT col1 FROM table2');
    const col = node.select_list[0] as ColumnRefExpression;
    expect(col.column_names).toEqual(['col1']);
    expect((node.from_table as BaseTableRef).table_name).toBe('table2');
  });

  it('parses line comment at end after semicolon', () => {
    const stmts = parse('SELECT 1; -- trailing comment');
    expect(stmts).toHaveLength(1);
  });

  it('parses INT keyword in CAST', () => {
    const node = selectNode('SELECT CAST(x AS INT) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.INTEGER);
  });

  it('parses BOOL keyword in CAST', () => {
    const node = selectNode('SELECT CAST(x AS BOOL) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.BOOLEAN);
  });

  it('parses FLOAT keyword in CAST', () => {
    const node = selectNode('SELECT CAST(x AS FLOAT) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.FLOAT);
  });

  it('parses TEXT keyword in CAST', () => {
    const node = selectNode('SELECT CAST(x AS TEXT) FROM t');
    const cast = node.select_list[0] as CastExpression;
    expect(cast.cast_type.id).toBe(LogicalTypeId.VARCHAR);
  });

  it('parses CREATE TABLE with single column, no constraints', () => {
    const stmt = parseOne('CREATE TABLE t (id INTEGER)') as CreateTableStatement;
    expect(stmt.columns).toHaveLength(1);
    expect(stmt.columns[0].is_primary_key).toBe(false);
    expect(stmt.columns[0].is_not_null).toBe(false);
    expect(stmt.columns[0].is_unique).toBe(false);
    expect(stmt.columns[0].default_value).toBeNull();
    expect(stmt.primary_key).toEqual([]);
    expect(stmt.foreign_keys).toEqual([]);
  });
});

// ============================================================================
// Edge cases: Feature interactions
// ============================================================================

describe('Feature interactions', () => {
  it('parses CTE + UNION', () => {
    const stmt = parseOne('WITH a AS (SELECT 1 AS x) SELECT x FROM a UNION SELECT 2');
    const setStmt = stmt as SetOperationStatement;
    expect(setStmt.node.type).toBe('SET_OPERATION_NODE');
    expect(setStmt.node.cte_map.map['a']).toBeDefined();
    expect(setStmt.node.set_op_type).toBe(SetOperationType.UNION);
  });

  it('parses INSERT...SELECT with JOIN', () => {
    const stmt = parseOne('INSERT INTO archive SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id') as InsertStatement;
    expect(stmt.select_statement).not.toBeNull();
    const innerNode = stmt.select_statement!.node as SelectNode;
    expect(innerNode.from_table!.type).toBe(TableRefType.JOIN);
  });

  it('parses UPDATE with subquery in SET value', () => {
    const stmt = parseOne('UPDATE t SET a = (SELECT MAX(b) FROM t2) WHERE id = 1') as UpdateStatement;
    const sub = stmt.set_clauses[0].value as SubqueryExpression;
    expect(sub.subquery_type).toBe('SCALAR');
  });

  it('parses UPDATE with IN in WHERE', () => {
    const stmt = parseOne('UPDATE t SET a = 1 WHERE id IN (1, 2, 3)') as UpdateStatement;
    const op = stmt.where_clause as OperatorExpression;
    expect(op.type).toBe(ExpressionType.OPERATOR_IN);
  });

  it('parses DELETE with subquery in WHERE', () => {
    const stmt = parseOne('DELETE FROM t WHERE id IN (SELECT id FROM expired)') as DeleteStatement;
    const sub = stmt.where_clause as SubqueryExpression;
    expect(sub.subquery_type).toBe('ANY');
  });

  it('parses DELETE with EXISTS in WHERE', () => {
    const stmt = parseOne('DELETE FROM t WHERE EXISTS (SELECT 1 FROM related WHERE related.t_id = t.id)') as DeleteStatement;
    const sub = stmt.where_clause as SubqueryExpression;
    expect(sub.subquery_type).toBe('EXISTS');
  });

  it('parses DISTINCT + ORDER BY + LIMIT combined', () => {
    const node = selectNode('SELECT DISTINCT name FROM t ORDER BY name LIMIT 5');
    const distinct = node.modifiers.find(m => m.type === ResultModifierType.DISTINCT_MODIFIER);
    const order = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER);
    const limit = node.modifiers.find(m => m.type === ResultModifierType.LIMIT_MODIFIER);
    expect(distinct).toBeDefined();
    expect(order).toBeDefined();
    expect(limit).toBeDefined();
  });

  it('parses subquery in HAVING', () => {
    const node = selectNode('SELECT dept FROM t GROUP BY dept HAVING dept IN (SELECT dept FROM important)');
    const sub = node.having as SubqueryExpression;
    expect(sub.subquery_type).toBe('ANY');
  });

  it('parses multiple CTEs where later CTE queries earlier CTE', () => {
    const stmt = parseSelect('WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a) SELECT * FROM b');
    const node = stmt.node as SelectNode;
    expect(node.cte_map.map['a']).toBeDefined();
    expect(node.cte_map.map['b']).toBeDefined();
    const bQuery = node.cte_map.map['b'].query.node as SelectNode;
    expect((bQuery.from_table as BaseTableRef).table_name).toBe('a');
  });

  it('parses UNION with DISTINCT on one side', () => {
    const stmt = parseOne('SELECT DISTINCT a FROM t1 UNION SELECT a FROM t2');
    const setStmt = stmt as SetOperationStatement;
    const left = setStmt.node.left as SelectNode;
    const distinct = left.modifiers.find(m => m.type === ResultModifierType.DISTINCT_MODIFIER);
    expect(distinct).toBeDefined();
  });

  it('parses nested subqueries in WHERE (two levels)', () => {
    const node = selectNode('SELECT * FROM t WHERE id IN (SELECT id FROM t2 WHERE val > (SELECT AVG(val) FROM t3))');
    const sub = node.where_clause as SubqueryExpression;
    expect(sub.subquery_type).toBe('ANY');
    const innerNode = sub.subquery.node as SelectNode;
    const innerCmp = innerNode.where_clause as ComparisonExpression;
    const innerSub = innerCmp.right as SubqueryExpression;
    expect(innerSub.subquery_type).toBe('SCALAR');
  });

  it('parses CROSS JOIN followed by LEFT JOIN', () => {
    const node = selectNode('SELECT * FROM a CROSS JOIN b LEFT JOIN c ON a.id = c.a_id');
    const outer = node.from_table as JoinRef;
    expect(outer.join_type).toBe(JoinType.LEFT);
    const inner = outer.left as JoinRef;
    expect(inner.join_type).toBe(JoinType.CROSS);
  });

  it('parses INSERT...SELECT with CTE', () => {
    const stmt = parseOne('INSERT INTO t WITH cte AS (SELECT 1 AS x) SELECT x FROM cte') as InsertStatement;
    expect(stmt.select_statement).not.toBeNull();
  });

  // ===========================================================================
  // Third pass: remaining untested code paths
  // ===========================================================================

  // --- IN with CTE subquery (base.ts line 320: check for WITH in IN) ---

  it('parses IN (WITH ... SELECT ...) — CTE inside IN subquery', () => {
    const node = selectNode(`
      SELECT * FROM t WHERE id IN (WITH cte AS (SELECT val FROM s) SELECT val FROM cte)
    `);
    const sub = node.where_clause as SubqueryExpression;
    expect(sub.expression_class).toBe(ExpressionClass.SUBQUERY);
    expect(sub.subquery_type).toBe('ANY');
    expect(sub.comparison_type).toBe(ExpressionType.COMPARE_EQUAL);
    expect(sub.child).not.toBeNull();
    const child = sub.child as ColumnRefExpression;
    expect(child.column_names).toEqual(['id']);
    // Verify the subquery has a CTE
    expect(sub.subquery.node.cte_map.map['cte']).toBeDefined();
  });

  it('parses NOT IN (WITH ... SELECT ...) — negated CTE inside IN subquery', () => {
    const node = selectNode(`
      SELECT * FROM t WHERE id NOT IN (WITH cte AS (SELECT val FROM s) SELECT val FROM cte)
    `);
    const notOp = node.where_clause as OperatorExpression;
    expect(notOp.type).toBe(ExpressionType.OPERATOR_NOT);
    const sub = notOp.children[0] as SubqueryExpression;
    expect(sub.subquery_type).toBe('ANY');
    expect(sub.comparison_type).toBe(ExpressionType.COMPARE_EQUAL);
    expect(sub.subquery.node.cte_map.map['cte']).toBeDefined();
  });

  // --- Scalar subquery with CTE (base.ts line 503: check for WITH) ---

  it('parses scalar subquery with CTE — (WITH ... SELECT ...)', () => {
    const node = selectNode(`
      SELECT (WITH cte AS (SELECT 42 AS v) SELECT v FROM cte) AS result
    `);
    const sub = node.select_list[0] as SubqueryExpression;
    expect(sub.expression_class).toBe(ExpressionClass.SUBQUERY);
    expect(sub.subquery_type).toBe('SCALAR');
    expect(sub.child).toBeNull();
    expect(sub.alias).toBe('result');
    expect(sub.subquery.node.cte_map.map['cte']).toBeDefined();
  });

  // --- comparison_type field on IN SubqueryExpression ---

  it('sets comparison_type to COMPARE_EQUAL on IN subquery expression', () => {
    const node = selectNode('SELECT * FROM t WHERE x IN (SELECT y FROM s)');
    const sub = node.where_clause as SubqueryExpression;
    expect(sub.comparison_type).toBe(ExpressionType.COMPARE_EQUAL);
  });

  // --- <> operator standalone test ---

  it('parses <> operator', () => {
    const node = selectNode('SELECT * FROM t WHERE a <> b');
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.type).toBe(ExpressionType.COMPARE_NOTEQUAL);
  });

  // --- NOT LIKE ---

  it('parses NOT LIKE', () => {
    const node = selectNode("SELECT * FROM t WHERE name NOT LIKE '%test%'");
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.expression_class).toBe(ExpressionClass.COMPARISON);
    expect(cmp.type).toBe(ExpressionType.COMPARE_NOT_LIKE);
  });

  // --- NOT BETWEEN with arithmetic expression bounds ---

  it('parses NOT BETWEEN with expression bounds', () => {
    const node = selectNode('SELECT * FROM t WHERE x NOT BETWEEN y + 1 AND z * 2');
    const notOp = node.where_clause as OperatorExpression;
    expect(notOp.type).toBe(ExpressionType.OPERATOR_NOT);
    const between = notOp.children[0] as BetweenExpression;
    expect(between.expression_class).toBe(ExpressionClass.BETWEEN);
    // lower bound is y + 1
    const lower = between.lower as OperatorExpression;
    expect(lower.type).toBe(ExpressionType.OPERATOR_ADD);
    // upper bound is z * 2
    const upper = between.upper as OperatorExpression;
    expect(upper.type).toBe(ExpressionType.OPERATOR_MULTIPLY);
  });

  // --- INSERT...SELECT with UNION ---

  it('parses INSERT...SELECT with UNION', () => {
    const stmt = parseOne('INSERT INTO t SELECT a FROM s1 UNION SELECT b FROM s2') as InsertStatement;
    expect(stmt.type).toBe(StatementType.INSERT_STATEMENT);
    expect(stmt.select_statement).not.toBeNull();
    const setStmt = stmt.select_statement as unknown as SetOperationStatement;
    expect(setStmt.node.type).toBe('SET_OPERATION_NODE');
  });

  // --- Column constraints in different orders ---

  it('parses column constraints in different orders: NOT NULL PRIMARY KEY UNIQUE DEFAULT', () => {
    const stmt = parseOne(`
      CREATE TABLE t (
        a INTEGER NOT NULL PRIMARY KEY UNIQUE DEFAULT 0,
        b TEXT UNIQUE NOT NULL DEFAULT 'x'
      )
    `) as CreateTableStatement;
    const colA = stmt.columns[0];
    expect(colA.is_not_null).toBe(true);
    expect(colA.is_primary_key).toBe(true);
    expect(colA.is_unique).toBe(true);
    expect(colA.default_value).not.toBeNull();
    const colB = stmt.columns[1];
    expect(colB.is_unique).toBe(true);
    expect(colB.is_not_null).toBe(true);
    expect(colB.default_value).not.toBeNull();
  });

  // --- JOIN ON with OR condition ---

  it('parses JOIN ON with OR condition', () => {
    const node = selectNode('SELECT * FROM a JOIN b ON a.id = b.a_id OR a.id2 = b.a_id2');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.INNER);
    const cond = join.condition as ConjunctionExpression;
    expect(cond.type).toBe(ExpressionType.CONJUNCTION_OR);
    expect(cond.children).toHaveLength(2);
  });

  // --- ParseError .token field verification ---

  it('ParseError includes token with line and column', () => {
    try {
      parse('SELECT * FROM WHERE');
      throw new Error('Should have thrown');
    } catch (e) {
      expect(e).toBeInstanceOf(ParseError);
      const pe = e as ParseError;
      expect(pe.line).toBeGreaterThan(0);
      expect(pe.column).toBeGreaterThan(0);
      expect(pe.token).toBeDefined();
      expect(pe.token.type).toBeDefined();
    }
  });

  // --- Error: Expected FIRST or LAST after NULLS ---

  it('throws for NULLS without FIRST or LAST', () => {
    expect(() => parse('SELECT * FROM t ORDER BY a NULLS')).toThrow(/FIRST.*LAST|Expected/);
  });

  // --- Error: Expected TABLE or INDEX after CREATE ---

  it('throws for CREATE without TABLE or INDEX', () => {
    expect(() => parse('CREATE foo')).toThrow(/TABLE.*INDEX|Expected/);
  });

  // --- Error: Expected ADD or DROP after ALTER TABLE ---

  it('throws for ALTER TABLE without ADD or DROP', () => {
    expect(() => parse('ALTER TABLE t RENAME x')).toThrow(/ADD.*DROP|Expected/);
  });

  // --- Error: Expected TABLE or INDEX after DROP ---

  it('throws for DROP without TABLE or INDEX', () => {
    expect(() => parse('DROP foo')).toThrow(/TABLE.*INDEX|Expected/);
  });

  // --- ALTER TABLE ADD/DROP without COLUMN keyword ---

  it('parses ALTER TABLE ADD without COLUMN keyword', () => {
    const stmt = parseOne('ALTER TABLE t ADD name TEXT') as AlterTableStatement;
    expect(stmt.alter_type).toBe(AlterType.ADD_COLUMN);
    expect(stmt.column_def!.name).toBe('name');
  });

  it('parses ALTER TABLE DROP without COLUMN keyword', () => {
    const stmt = parseOne('ALTER TABLE t DROP name') as AlterTableStatement;
    expect(stmt.alter_type).toBe(AlterType.DROP_COLUMN);
    expect(stmt.column_name).toBe('name');
  });

  // --- DROP INDEX ---

  it('parses DROP INDEX without IF EXISTS', () => {
    const stmt = parseOne('DROP INDEX idx_name') as DropStatement;
    expect(stmt.drop_type).toBe(DropType.INDEX);
    expect(stmt.name).toBe('idx_name');
    expect(stmt.if_exists).toBe(false);
  });

  it('parses DROP INDEX IF EXISTS', () => {
    const stmt = parseOne('DROP INDEX IF EXISTS idx_name') as DropStatement;
    expect(stmt.drop_type).toBe(DropType.INDEX);
    expect(stmt.name).toBe('idx_name');
    expect(stmt.if_exists).toBe(true);
  });

  // --- Type with size: VARCHAR(255), CHAR(10) in CREATE TABLE ---

  it('parses type with size in CREATE TABLE column', () => {
    const stmt = parseOne('CREATE TABLE t (a VARCHAR(100), b CHAR(50))') as CreateTableStatement;
    expect(stmt.columns[0].type.id).toBe(LogicalTypeId.VARCHAR);
    expect(stmt.columns[1].type.id).toBe(LogicalTypeId.VARCHAR);
  });

  // --- Table alias without AS keyword ---

  it('parses table alias without AS keyword', () => {
    const node = selectNode('SELECT t.x FROM mytable t');
    const table = node.from_table as BaseTableRef;
    expect(table.table_name).toBe('mytable');
    expect(table.alias).toBe('t');
  });

  // --- Subquery in FROM alias without AS ---

  it('parses subquery in FROM with alias without AS keyword', () => {
    const node = selectNode('SELECT * FROM (SELECT 1 AS x) sub');
    const table = node.from_table as SubqueryRef;
    expect(table.type).toBe(TableRefType.SUBQUERY);
    expect(table.alias).toBe('sub');
  });

  // --- Zero-argument function ---

  it('parses zero-argument function call', () => {
    const node = selectNode('SELECT now() FROM t');
    const fn = node.select_list[0] as FunctionExpression;
    expect(fn.function_name).toBe('now');
    expect(fn.children).toHaveLength(0);
    expect(fn.is_star).toBe(false);
    expect(fn.distinct).toBe(false);
  });

  // --- Function with multiple arguments ---

  it('parses function with multiple arguments', () => {
    const node = selectNode('SELECT coalesce(a, b, c) FROM t');
    const fn = node.select_list[0] as FunctionExpression;
    expect(fn.function_name).toBe('coalesce');
    expect(fn.children).toHaveLength(3);
  });

  // --- Simple CASE with multiple WHEN ---

  it('parses simple CASE with multiple WHEN and ELSE', () => {
    const node = selectNode("SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END FROM t");
    const caseExpr = node.select_list[0] as CaseExpression;
    expect(caseExpr.case_checks).toHaveLength(2);
    // Each WHEN is desugared to equality comparison with status
    const when0 = caseExpr.case_checks[0].when_expr as ComparisonExpression;
    expect(when0.type).toBe(ExpressionType.COMPARE_EQUAL);
    const leftCol = when0.left as ColumnRefExpression;
    expect(leftCol.column_names).toEqual(['status']);
    const when1 = caseExpr.case_checks[1].when_expr as ComparisonExpression;
    expect(when1.type).toBe(ExpressionType.COMPARE_EQUAL);
    expect(caseExpr.else_expr).not.toBeNull();
  });

  // --- SELECT without FROM ---

  it('parses SELECT without FROM', () => {
    const node = selectNode('SELECT 1, 2, 3');
    expect(node.from_table).toBeNull();
    expect(node.select_list).toHaveLength(3);
  });

  // --- OFFSET before LIMIT (reversed) with value verification ---

  it('parses OFFSET before LIMIT and verifies values', () => {
    const node = selectNode('SELECT * FROM t OFFSET 20 LIMIT 10');
    const limitMod = node.modifiers.find(m => m.type === ResultModifierType.LIMIT_MODIFIER) as LimitModifier;
    const limit = limitMod.limit as ConstantExpression;
    expect(limit.value.value).toBe(10);
    const offset = limitMod.offset as ConstantExpression;
    expect(offset.value.value).toBe(20);
  });

  // --- RIGHT OUTER JOIN ---

  it('parses RIGHT OUTER JOIN', () => {
    const node = selectNode('SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.RIGHT);
  });

  // --- Plain JOIN is INNER ---

  it('parses plain JOIN as INNER', () => {
    const node = selectNode('SELECT * FROM a JOIN b ON a.id = b.id');
    const join = node.from_table as JoinRef;
    expect(join.join_type).toBe(JoinType.INNER);
  });

  // --- SELECT expression alias without AS ---

  it('parses select expression alias without AS keyword', () => {
    const node = selectNode('SELECT x myalias FROM t');
    expect(node.select_list[0].alias).toBe('myalias');
  });

  // --- Multiple OR children flattened ---

  it('flattens multiple OR children into single conjunction', () => {
    const node = selectNode('SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3');
    const conj = node.where_clause as ConjunctionExpression;
    expect(conj.type).toBe(ExpressionType.CONJUNCTION_OR);
    expect(conj.children).toHaveLength(3);
  });

  // --- Multiple AND children flattened ---

  it('flattens multiple AND children into single conjunction', () => {
    const node = selectNode('SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3');
    const conj = node.where_clause as ConjunctionExpression;
    expect(conj.type).toBe(ExpressionType.CONJUNCTION_AND);
    expect(conj.children).toHaveLength(3);
  });

  // --- CREATE UNIQUE INDEX ---

  it('parses CREATE UNIQUE INDEX', () => {
    const stmt = parseOne('CREATE UNIQUE INDEX idx ON t (a, b)') as CreateIndexStatement;
    expect(stmt.is_unique).toBe(true);
    expect(stmt.index_name).toBe('idx');
    expect(stmt.columns).toEqual(['a', 'b']);
  });

  // --- CREATE INDEX IF NOT EXISTS ---

  it('parses CREATE INDEX IF NOT EXISTS', () => {
    const stmt = parseOne('CREATE INDEX IF NOT EXISTS idx ON t (a)') as CreateIndexStatement;
    expect(stmt.if_not_exists).toBe(true);
  });

  // --- Error: Expected expression after SELECT ---

  it('throws for SELECT followed by FROM immediately (empty select list)', () => {
    expect(() => parse('SELECT FROM t')).toThrow(/Expected expression/);
  });

  it('throws for bare SELECT at EOF (empty select list with EOF)', () => {
    expect(() => parse('SELECT')).toThrow(/Expected expression.*EOF/);
  });

  // --- Error: Expected BY after ORDER ---

  it('throws for ORDER without BY', () => {
    expect(() => parse('SELECT * FROM t ORDER x')).toThrow(/BY/);
  });

  // --- Error: Expected BY after GROUP ---

  it('throws for GROUP without BY', () => {
    expect(() => parse('SELECT * FROM t GROUP x')).toThrow(/BY/);
  });

  // --- Error: Expected type name ---

  it('throws for CAST with invalid type', () => {
    expect(() => parse('SELECT CAST(x AS FOOBAR) FROM t')).toThrow(/type/i);
  });

  // --- Error: Expected AS in CAST ---

  it('throws for CAST without AS', () => {
    expect(() => parse('SELECT CAST(x INTEGER) FROM t')).toThrow(/AS/);
  });

  // --- Error: Expected THEN after WHEN ---

  it('throws for CASE WHEN without THEN', () => {
    expect(() => parse('SELECT CASE WHEN x = 1 END FROM t')).toThrow(/THEN/);
  });

  // --- Error: Expected END after CASE ---

  it('throws for CASE without END', () => {
    expect(() => parse('SELECT CASE WHEN x = 1 THEN 2 FROM t')).toThrow(/END/);
  });

  // --- Error: Expected INTO after INSERT ---

  it('throws for INSERT without INTO', () => {
    expect(() => parse('INSERT t VALUES (1)')).toThrow(/INTO/);
  });

  // --- Error: Expected SET after UPDATE ---

  it('throws for UPDATE without SET', () => {
    expect(() => parse('UPDATE t WHERE x = 1')).toThrow(/SET/);
  });

  // --- Error: Expected FROM after DELETE ---

  it('throws for DELETE without FROM', () => {
    expect(() => parse('DELETE t WHERE x = 1')).toThrow(/FROM/);
  });

  // --- Error: Expected JOIN after CROSS ---

  it('throws for CROSS without JOIN', () => {
    expect(() => parse('SELECT * FROM a CROSS b')).toThrow(/JOIN/);
  });

  // --- Error: Expected table name after FROM ---

  it('throws for FROM with no table name', () => {
    expect(() => parse('SELECT * FROM WHERE x = 1')).toThrow();
  });

  // --- Error: Expected VALUES or SELECT after INSERT INTO ---

  it('throws for INSERT INTO t without VALUES or SELECT', () => {
    expect(() => parse('INSERT INTO t (a)')).toThrow(/VALUES.*SELECT|Expected/);
  });

  // --- Multiple value rows in INSERT ---

  it('parses INSERT with multiple value rows and verifies all rows', () => {
    const stmt = parseOne('INSERT INTO t (a, b) VALUES (1, 2), (3, 4), (5, 6)') as InsertStatement;
    expect(stmt.values).toHaveLength(3);
    const v0 = stmt.values[0][0] as ConstantExpression;
    expect(v0.value.value).toBe(1);
    const v2 = stmt.values[2][0] as ConstantExpression;
    expect(v2.value.value).toBe(5);
  });

  // --- Multiple SET clauses in UPDATE ---

  it('parses UPDATE with multiple SET clauses', () => {
    const stmt = parseOne('UPDATE t SET a = 1, b = 2, c = 3 WHERE id = 1') as UpdateStatement;
    expect(stmt.set_clauses).toHaveLength(3);
    expect(stmt.set_clauses[0].column).toBe('a');
    expect(stmt.set_clauses[1].column).toBe('b');
    expect(stmt.set_clauses[2].column).toBe('c');
  });

  // --- Table-level PRIMARY KEY with multiple columns ---

  it('parses table-level composite PRIMARY KEY', () => {
    const stmt = parseOne('CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))') as CreateTableStatement;
    expect(stmt.primary_key).toEqual(['a', 'b']);
  });

  // --- Multiple foreign keys ---

  it('parses CREATE TABLE with multiple foreign keys', () => {
    const stmt = parseOne(`
      CREATE TABLE t (
        a INTEGER,
        b INTEGER,
        FOREIGN KEY (a) REFERENCES r1 (id),
        FOREIGN KEY (b) REFERENCES r2 (id)
      )
    `) as CreateTableStatement;
    expect(stmt.foreign_keys).toHaveLength(2);
    expect(stmt.foreign_keys[0].ref_table).toBe('r1');
    expect(stmt.foreign_keys[1].ref_table).toBe('r2');
  });

  // --- SELECT ALL (no distinct) ---

  it('parses SELECT without DISTINCT (no distinct modifier)', () => {
    const node = selectNode('SELECT a FROM t');
    const distinctMod = node.modifiers.find(m => m.type === ResultModifierType.DISTINCT_MODIFIER);
    expect(distinctMod).toBeUndefined();
  });

  // --- Nested parenthesized expression ---

  it('parses nested parenthesized expressions', () => {
    const node = selectNode('SELECT ((a + b)) FROM t');
    const op = node.select_list[0] as OperatorExpression;
    expect(op.type).toBe(ExpressionType.OPERATOR_ADD);
  });

  // --- Multiple ORDER BY columns ---

  it('parses ORDER BY with multiple columns and mixed directions', () => {
    const node = selectNode('SELECT * FROM t ORDER BY a ASC, b DESC, c');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders).toHaveLength(3);
    expect(orderMod.orders[0].type).toBe(OrderType.ASCENDING);
    expect(orderMod.orders[1].type).toBe(OrderType.DESCENDING);
    expect(orderMod.orders[2].type).toBe(OrderType.ASCENDING); // default
  });

  // --- NULLS FIRST / NULLS LAST override defaults ---

  it('parses NULLS FIRST on ASC (overrides default NULLS LAST)', () => {
    const node = selectNode('SELECT * FROM t ORDER BY a ASC NULLS FIRST');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].null_order).toBe(OrderByNullType.NULLS_FIRST);
  });

  it('parses NULLS LAST on DESC (overrides default NULLS FIRST)', () => {
    const node = selectNode('SELECT * FROM t ORDER BY a DESC NULLS LAST');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].null_order).toBe(OrderByNullType.NULLS_LAST);
  });

  // --- Default null order ---

  it('defaults to NULLS LAST for ASC', () => {
    const node = selectNode('SELECT * FROM t ORDER BY a ASC');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].null_order).toBe(OrderByNullType.NULLS_LAST);
  });

  it('defaults to NULLS FIRST for DESC', () => {
    const node = selectNode('SELECT * FROM t ORDER BY a DESC');
    const orderMod = node.modifiers.find(m => m.type === ResultModifierType.ORDER_MODIFIER) as OrderModifier;
    expect(orderMod.orders[0].null_order).toBe(OrderByNullType.NULLS_FIRST);
  });

  // --- UNION ORDER BY LIMIT on set operation ---

  it('parses UNION with ORDER BY and LIMIT on the set operation', () => {
    const stmt = parseOne('SELECT a FROM t1 UNION SELECT b FROM t2 ORDER BY a LIMIT 5') as SetOperationStatement;
    const setNode = stmt.node as SetOperationNode;
    expect(setNode.modifiers).toHaveLength(2); // ORDER + LIMIT
    expect(setNode.modifiers[0].type).toBe(ResultModifierType.ORDER_MODIFIER);
    expect(setNode.modifiers[1].type).toBe(ResultModifierType.LIMIT_MODIFIER);
  });

  // --- Quoted identifier in expressions ---

  it('parses quoted identifier in column ref', () => {
    const node = selectNode('SELECT "my column" FROM t');
    const col = node.select_list[0] as ColumnRefExpression;
    expect(col.column_names).toEqual(['my column']);
  });

  // --- Quoted identifier in table.column ---

  it('parses quoted identifier in table.column', () => {
    const node = selectNode('SELECT t."my col" FROM t');
    const col = node.select_list[0] as ColumnRefExpression;
    expect(col.column_names).toEqual(['t', 'my col']);
  });

  // --- Error: unterminated string at specific line/col ---

  it('ParseError for unterminated string includes correct line number', () => {
    try {
      parse("SELECT 'hello");
      throw new Error('Should have thrown');
    } catch (e) {
      const pe = e as ParseError;
      expect(pe.line).toBe(1);
      expect(pe.column).toBe(8); // opening quote position
    }
  });

  // --- Multiple semicolons between statements ---

  it('parses statements separated by multiple semicolons', () => {
    const stmts = parse('SELECT 1;;; SELECT 2');
    expect(stmts).toHaveLength(2);
  });

  // --- Empty string input ---

  it('parses empty string input', () => {
    const stmts = parse('');
    expect(stmts).toHaveLength(0);
  });

  // --- Only semicolons ---

  it('parses input with only semicolons', () => {
    const stmts = parse(';;;');
    expect(stmts).toHaveLength(0);
  });

  // --- Only whitespace ---

  it('parses input with only whitespace', () => {
    const stmts = parse('   \n\t  ');
    expect(stmts).toHaveLength(0);
  });

  // --- CTE column aliases ---

  it('parses CTE with column aliases', () => {
    const node = selectNode('WITH cte(x, y) AS (SELECT a, b FROM t) SELECT x, y FROM cte');
    const cte = node.cte_map.map['cte'];
    expect(cte).toBeDefined();
    expect(cte.aliases).toEqual(['x', 'y']);
  });

  // --- EXISTS subquery child field is null ---

  it('EXISTS subquery has null child field', () => {
    const node = selectNode('SELECT * FROM t WHERE EXISTS (SELECT 1 FROM s)');
    const sub = node.where_clause as SubqueryExpression;
    expect(sub.subquery_type).toBe('EXISTS');
    expect(sub.child).toBeNull();
  });

  // --- NOT EXISTS subquery ---

  it('NOT EXISTS subquery has NOT_EXISTS type', () => {
    const node = selectNode('SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM s)');
    const sub = node.where_clause as SubqueryExpression;
    expect(sub.subquery_type).toBe('NOT_EXISTS');
    expect(sub.child).toBeNull();
  });

  // --- schema_name is always null for BaseTableRef ---

  it('BaseTableRef always has null schema_name', () => {
    const node = selectNode('SELECT * FROM mytable');
    const table = node.from_table as BaseTableRef;
    expect(table.schema_name).toBeNull();
  });

  // --- SubqueryRef column_name_alias is empty ---

  it('SubqueryRef has empty column_name_alias', () => {
    const node = selectNode('SELECT * FROM (SELECT 1 AS x) AS sub');
    const table = node.from_table as SubqueryRef;
    expect(table.column_name_alias).toEqual([]);
  });

  // --- GROUP BY with multiple expressions ---

  it('parses GROUP BY with multiple expressions', () => {
    const node = selectNode('SELECT a, b, COUNT(*) FROM t GROUP BY a, b');
    expect(node.groups.group_expressions).toHaveLength(2);
  });

  // --- Parameter expressions ---

  it('parses $1 parameter', () => {
    const node = selectNode('SELECT * FROM t WHERE x = $1');
    const cmp = node.where_clause as ComparisonExpression;
    expect(cmp.right.expression_class).toBe(ExpressionClass.PARAMETER);
    expect((cmp.right as any).index).toBe(0); // $1 → 0-based index 0
  });

  it('parses multiple parameters $1, $2', () => {
    const node = selectNode('SELECT * FROM t WHERE x > $1 AND x < $2');
    const conj = node.where_clause as ConjunctionExpression;
    const left = conj.children[0] as ComparisonExpression;
    const right = conj.children[1] as ComparisonExpression;
    expect(left.right.expression_class).toBe(ExpressionClass.PARAMETER);
    expect((left.right as any).index).toBe(0);
    expect(right.right.expression_class).toBe(ExpressionClass.PARAMETER);
    expect((right.right as any).index).toBe(1);
  });

  it('$0 parameter throws ParseError', () => {
    expect(() => parse('SELECT * FROM t WHERE x = $0')).toThrow();
  });

  // --- Error reporting ---

  it('empty query returns no statements', () => {
    expect(parse('')).toHaveLength(0);
  });

  it('error on dangling AND', () => {
    expect(() => parse('SELECT * FROM t WHERE x > 1 AND')).toThrow();
  });

  it('error on double comma in SELECT list', () => {
    expect(() => parse('SELECT a,, b FROM t')).toThrow();
  });

  it('error on missing table after JOIN', () => {
    expect(() => parse('SELECT * FROM t JOIN')).toThrow();
  });

  it('error on INSERT without VALUES or SELECT', () => {
    expect(() => parse('INSERT INTO t (a, b)')).toThrow();
  });

  // --- HAVING without GROUP BY ---

  it('parses HAVING without GROUP BY', () => {
    const node = selectNode('SELECT COUNT(*) FROM t HAVING COUNT(*) > 5');
    expect(node.having).not.toBeNull();
    expect(node.groups.group_expressions).toHaveLength(0);
  });

  // --- Multiple statements ---

  it('parses multiple statements separated by semicolon', () => {
    const stmts = parse('SELECT 1; SELECT 2');
    expect(stmts).toHaveLength(2);
  });

  // --- DELETE without WHERE ---

  it('parses DELETE without WHERE', () => {
    const stmt = parseOne('DELETE FROM t');
    expect(stmt.type).toBe(StatementType.DELETE_STATEMENT);
    expect((stmt as DeleteStatement).where_clause).toBeNull();
  });

  // --- UPDATE without WHERE ---

  it('parses UPDATE without WHERE', () => {
    const stmt = parseOne('UPDATE t SET x = 1');
    expect(stmt.type).toBe(StatementType.UPDATE_STATEMENT);
    expect((stmt as UpdateStatement).where_clause).toBeNull();
  });

  // --- INSERT without column list ---

  it('parses INSERT without column list', () => {
    const stmt = parseOne('INSERT INTO t VALUES (1, 2, 3)');
    expect(stmt.type).toBe(StatementType.INSERT_STATEMENT);
    expect((stmt as InsertStatement).columns).toHaveLength(0);
  });

  // --- CASE with operand form ---

  it('parses CASE operand form (CASE expr WHEN val THEN ...)', () => {
    const node = selectNode('SELECT CASE x WHEN 1 THEN \'one\' WHEN 2 THEN \'two\' ELSE \'other\' END FROM t');
    const caseExpr = node.select_list[0] as CaseExpression;
    expect(caseExpr.expression_class).toBe(ExpressionClass.CASE);
    // Simple CASE has operand
    expect(caseExpr.case_operand).not.toBeNull();
    expect(caseExpr.case_checks.length).toBe(2);
    expect(caseExpr.else_expr).not.toBeNull();
  });

  // --- CAST to BIGINT ---

  it('parses CAST to BIGINT', () => {
    const node = selectNode('SELECT CAST(x AS BIGINT) FROM t');
    const castExpr = node.select_list[0] as CastExpression;
    expect(castExpr.expression_class).toBe(ExpressionClass.CAST);
    expect(castExpr.cast_type.id).toBe(LogicalTypeId.BIGINT);
  });

  // --- CAST to BLOB ---

  it('parses CAST to BLOB', () => {
    const node = selectNode('SELECT CAST(x AS BLOB) FROM t');
    const castExpr = node.select_list[0] as CastExpression;
    expect(castExpr.expression_class).toBe(ExpressionClass.CAST);
    expect(castExpr.cast_type.id).toBe(LogicalTypeId.BLOB);
  });

  // --- NOT BETWEEN ---

  it('parses NOT BETWEEN', () => {
    const node = selectNode('SELECT * FROM t WHERE x NOT BETWEEN 1 AND 10');
    expect(node.where_clause).not.toBeNull();
    // NOT BETWEEN wraps BETWEEN in a NOT operator
    const where = node.where_clause! as OperatorExpression;
    expect(where.type).toBe(ExpressionType.OPERATOR_NOT);
    const between = where.children[0] as BetweenExpression;
    expect(between.expression_class).toBe(ExpressionClass.BETWEEN);
  });

  // --- Recursive CTE with column aliases ---

  it('parses WITH RECURSIVE with column aliases', () => {
    const stmt = parseOne('WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT n FROM r') as SelectStatement;
    expect(stmt.type).toBe(StatementType.SELECT_STATEMENT);
    expect(stmt.node.cte_map.recursive).toBe(true);
    expect(stmt.node.cte_map.map['r']).toBeDefined();
    expect(stmt.node.cte_map.map['r'].aliases).toEqual(['n']);
  });
});
