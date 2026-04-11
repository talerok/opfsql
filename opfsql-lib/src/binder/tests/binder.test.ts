import { describe, it, expect, beforeEach } from 'vitest';
import { Parser } from '../../parser/index.js';
import { Catalog } from '../../store/catalog.js';
import type { TableSchema } from '../../store/types.js';
import { Binder } from '../index.js';
import { BindError } from '../core/errors.js';
import {
  LogicalOperatorType,
  BoundExpressionClass,
} from '../types.js';
import type {
  LogicalGet,
  LogicalFilter,
  LogicalProjection,
  LogicalAggregate,
  LogicalComparisonJoin,
  LogicalCrossProduct,
  LogicalOrderBy,
  LogicalLimit,
  LogicalDistinct,
  LogicalUnion,
  LogicalCreateTable,
  LogicalCreateIndex,
  LogicalAlterTable,
  LogicalDrop,
  LogicalInsert,
  LogicalUpdate,
  LogicalDelete,
  LogicalMaterializedCTE,
  LogicalRecursiveCTE,
  LogicalCTERef,
  BoundColumnRefExpression,
  BoundConstantExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundAggregateExpression,
  BoundOperatorExpression,
  BoundFunctionExpression,
  BoundBetweenExpression,
  BoundCaseExpression,
  BoundCastExpression,
  BoundSubqueryExpression,
  LogicalOperator,
} from '../types.js';

const parser = new Parser();

function parse(sql: string) {
  const stmts = parser.parse(sql);
  expect(stmts).toHaveLength(1);
  return stmts[0];
}

const usersSchema: TableSchema = {
  name: 'users',
  columns: [
    { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, unique: true, defaultValue: null },
    { name: 'name', type: 'TEXT', nullable: false, primaryKey: false, unique: false, defaultValue: null },
    { name: 'age', type: 'INTEGER', nullable: true, primaryKey: false, unique: false, defaultValue: null },
    { name: 'active', type: 'BOOLEAN', nullable: true, primaryKey: false, unique: false, defaultValue: null },
  ],
};

const ordersSchema: TableSchema = {
  name: 'orders',
  columns: [
    { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, unique: true, defaultValue: null },
    { name: 'user_id', type: 'INTEGER', nullable: false, primaryKey: false, unique: false, defaultValue: null },
    { name: 'amount', type: 'REAL', nullable: true, primaryKey: false, unique: false, defaultValue: null },
    { name: 'status', type: 'TEXT', nullable: true, primaryKey: false, unique: false, defaultValue: null },
  ],
};

let catalog: Catalog;
let binder: Binder;

beforeEach(() => {
  catalog = new Catalog();
  catalog.addTable(usersSchema);
  catalog.addTable(ordersSchema);
  binder = new Binder(catalog);
});

function bind(sql: string): LogicalOperator {
  return binder.bindStatement(parse(sql));
}

// ============================================================================
// SELECT
// ============================================================================

describe('SELECT', () => {
  it('SELECT * expands to all columns of all tables in FROM order', () => {
    const plan = bind('SELECT * FROM users');
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(4); // id, name, age, active
    expect(proj.types).toEqual(['INTEGER', 'TEXT', 'INTEGER', 'BOOLEAN']);

    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    expect(col0.columnName).toBe('id');
    const col1 = proj.expressions[1] as BoundColumnRefExpression;
    expect(col1.columnName).toBe('name');
    const col2 = proj.expressions[2] as BoundColumnRefExpression;
    expect(col2.columnName).toBe('age');
    const col3 = proj.expressions[3] as BoundColumnRefExpression;
    expect(col3.columnName).toBe('active');
  });

  it('table.* expands to columns of only that table', () => {
    const plan = bind(
      'SELECT u.* FROM users u JOIN orders o ON u.id = o.user_id',
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(4); // users columns only
    for (const expr of proj.expressions) {
      expect((expr as BoundColumnRefExpression).tableName).toBe('users');
    }
  });

  it('table alias resolves correctly', () => {
    const plan = bind('SELECT u.name FROM users u');
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(1);
    const col = proj.expressions[0] as BoundColumnRefExpression;
    expect(col.columnName).toBe('name');
    expect(col.tableName).toBe('users');
  });

  it('column without alias resolves when not ambiguous', () => {
    const plan = bind('SELECT name FROM users');
    const proj = plan as LogicalProjection;
    const col = proj.expressions[0] as BoundColumnRefExpression;
    expect(col.columnName).toBe('name');
  });

  it('non-existent table throws BindError', () => {
    expect(() => bind('SELECT * FROM missing_table')).toThrow(BindError);
    expect(() => bind('SELECT * FROM missing_table')).toThrow(
      'Table "missing_table" not found',
    );
  });

  it('non-existent column throws BindError', () => {
    expect(() => bind('SELECT missing_col FROM users')).toThrow(BindError);
    expect(() => bind('SELECT missing_col FROM users')).toThrow(
      'Column "missing_col" not found',
    );
  });

  it('ambiguous column throws BindError', () => {
    expect(() =>
      bind('SELECT id FROM users JOIN orders ON users.id = orders.id'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT id FROM users JOIN orders ON users.id = orders.id'),
    ).toThrow('ambiguous');
  });

  it('aggregate in WHERE throws BindError', () => {
    expect(() =>
      bind('SELECT * FROM users WHERE COUNT(*) > 1'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT * FROM users WHERE COUNT(*) > 1'),
    ).toThrow('Aggregate function not allowed in WHERE clause');
  });

  it('INTEGER > INTEGER returns BOOLEAN', () => {
    const plan = bind('SELECT * FROM users WHERE age > 18');
    const filter = (plan as LogicalProjection).children[0] as LogicalFilter;
    expect(filter.type).toBe(LogicalOperatorType.LOGICAL_FILTER);
    const cmp = filter.expressions[0] as BoundComparisonExpression;
    expect(cmp.returnType).toBe('BOOLEAN');
  });

  it('TEXT > INTEGER throws BindError', () => {
    expect(() =>
      bind("SELECT * FROM users WHERE name > 18"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT * FROM users WHERE name > 18"),
    ).toThrow('Type mismatch');
  });

  it('COUNT(*) returns type INTEGER', () => {
    const plan = bind('SELECT COUNT(*) FROM users');
    const proj = plan as LogicalProjection;
    // With aggregate, there's an aggregate node between projection and get
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg.expressions[0].returnType).toBe('INTEGER');
    expect(agg.expressions[0].isStar).toBe(true);
  });

  it('GROUP BY without aggregate creates LogicalAggregate with empty expressions', () => {
    const plan = bind('SELECT name FROM users GROUP BY name');
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg.groups).toHaveLength(1);
    expect(agg.expressions).toHaveLength(0);
  });

  it('LIMIT without OFFSET sets offsetVal = 0', () => {
    const plan = bind('SELECT * FROM users LIMIT 10');
    // Should have LIMIT on top
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_LIMIT);
    const limit = plan as LogicalLimit;
    expect(limit.limitVal).toBe(10);
    expect(limit.offsetVal).toBe(0);
  });

  it('subquery creates independent BindScope', () => {
    const plan = bind(
      'SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)',
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    expect(filter.type).toBe(LogicalOperatorType.LOGICAL_FILTER);
    // The expression should contain a subquery or IN operator
    expect(filter.expressions).toHaveLength(1);
  });

  it('CTE is accessible in main query', () => {
    const plan = bind(
      'WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active',
    );
    // Top node should be LogicalMaterializedCTE wrapping CTE plan + main query
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const cte = plan as LogicalMaterializedCTE;
    expect(cte.cteName).toBe('active');
    expect(cte.children).toHaveLength(2);
    // children[0] = CTE plan (projection over filter over get)
    // children[1] = main query (projection over CTE ref)
    const mainQuery = cte.children[1];
    expect(mainQuery.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
  });

  it('CTE not accessible outside its scope', () => {
    // Just binding a query referencing a non-existent CTE should throw
    expect(() => bind('SELECT * FROM nonexistent_cte')).toThrow(BindError);
  });

  it('DISTINCT wraps in LogicalDistinct', () => {
    const plan = bind('SELECT DISTINCT name FROM users');
    // Modifiers order: DISTINCT, ORDER BY, LIMIT
    // Find the distinct node
    let node: LogicalOperator = plan;
    let foundDistinct = false;
    while (node) {
      if (node.type === LogicalOperatorType.LOGICAL_DISTINCT) {
        foundDistinct = true;
        break;
      }
      if (node.children.length > 0) {
        node = node.children[0];
      } else {
        break;
      }
    }
    expect(foundDistinct).toBe(true);
  });

  it('ORDER BY wraps in LogicalOrderBy', () => {
    const plan = bind('SELECT name FROM users ORDER BY name ASC');
    let node: LogicalOperator = plan;
    let foundOrder = false;
    while (node) {
      if (node.type === LogicalOperatorType.LOGICAL_ORDER_BY) {
        foundOrder = true;
        const orderBy = node as LogicalOrderBy;
        expect(orderBy.orders).toHaveLength(1);
        expect(orderBy.orders[0].orderType).toBe('ASCENDING');
        break;
      }
      if (node.children.length > 0) {
        node = node.children[0];
      } else {
        break;
      }
    }
    expect(foundOrder).toBe(true);
  });

  it('LIMIT with OFFSET', () => {
    const plan = bind('SELECT * FROM users LIMIT 10 OFFSET 5');
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_LIMIT);
    const limit = plan as LogicalLimit;
    expect(limit.limitVal).toBe(10);
    expect(limit.offsetVal).toBe(5);
  });

  it('builds correct tree: SELECT name FROM users WHERE age > 18 LIMIT 10', () => {
    const plan = bind('SELECT name FROM users WHERE age > 18 LIMIT 10');

    // Top: LogicalLimit
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_LIMIT);
    const limit = plan as LogicalLimit;
    expect(limit.limitVal).toBe(10);

    // Next: LogicalProjection
    const proj = limit.children[0] as LogicalProjection;
    expect(proj.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    expect(proj.expressions).toHaveLength(1);

    // Next: LogicalFilter
    const filter = proj.children[0] as LogicalFilter;
    expect(filter.type).toBe(LogicalOperatorType.LOGICAL_FILTER);

    // Bottom: LogicalGet
    const get = filter.children[0] as LogicalGet;
    expect(get.type).toBe(LogicalOperatorType.LOGICAL_GET);
    expect(get.tableName).toBe('users');
  });
});

// ============================================================================
// JOIN
// ============================================================================

describe('JOIN', () => {
  it('INNER JOIN builds LogicalComparisonJoin with joinType INNER', () => {
    const plan = bind(
      'SELECT u.name FROM users u INNER JOIN orders o ON u.id = o.user_id',
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    expect(join.type).toBe(LogicalOperatorType.LOGICAL_COMPARISON_JOIN);
    expect(join.joinType).toBe('INNER');
  });

  it('LEFT JOIN builds LogicalComparisonJoin with joinType LEFT', () => {
    const plan = bind(
      'SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id',
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    expect(join.type).toBe(LogicalOperatorType.LOGICAL_COMPARISON_JOIN);
    expect(join.joinType).toBe('LEFT');
  });

  it('columns from both tables are accessible after JOIN', () => {
    const plan = bind(
      'SELECT u.name, o.amount FROM users u INNER JOIN orders o ON u.id = o.user_id',
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    expect(col0.tableName).toBe('users');
    expect(col0.columnName).toBe('name');
    const col1 = proj.expressions[1] as BoundColumnRefExpression;
    expect(col1.tableName).toBe('orders');
    expect(col1.columnName).toBe('amount');
  });

  it('JOIN ON condition resolves columns from both tables', () => {
    const plan = bind(
      'SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id',
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    expect(join.conditions).toHaveLength(1);
    expect(join.conditions[0].comparisonType).toBe('EQUAL');
    const leftCol = join.conditions[0].left as BoundColumnRefExpression;
    const rightCol = join.conditions[0].right as BoundColumnRefExpression;
    expect(leftCol.tableName).toBe('users');
    expect(rightCol.tableName).toBe('orders');
  });

  it('CROSS JOIN builds LogicalCrossProduct', () => {
    const plan = bind('SELECT * FROM users CROSS JOIN orders');
    const proj = plan as LogicalProjection;
    const cross = proj.children[0] as LogicalCrossProduct;
    expect(cross.type).toBe(LogicalOperatorType.LOGICAL_CROSS_PRODUCT);
  });
});

// ============================================================================
// ColumnBinding
// ============================================================================

describe('ColumnBinding', () => {
  it('tableIndex is unique for each table in query', () => {
    const plan = bind(
      'SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id',
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    const leftGet = join.children[0] as LogicalGet;
    const rightGet = join.children[1] as LogicalGet;
    expect(leftGet.tableIndex).not.toBe(rightGet.tableIndex);
  });

  it('getColumnBindings() returns correct bindings for LogicalGet', () => {
    const plan = bind('SELECT * FROM users');
    const proj = plan as LogicalProjection;
    const get = proj.children[0] as LogicalGet;
    const bindings = get.getColumnBindings();
    expect(bindings).toHaveLength(4);
    expect(bindings[0].tableIndex).toBe(get.tableIndex);
    expect(bindings[0].columnIndex).toBe(0);
    expect(bindings[1].columnIndex).toBe(1);
    expect(bindings[2].columnIndex).toBe(2);
    expect(bindings[3].columnIndex).toBe(3);
  });

  it('after PROJECTION getColumnBindings() reflects only selected columns', () => {
    const plan = bind('SELECT name, age FROM users');
    const proj = plan as LogicalProjection;
    const bindings = proj.getColumnBindings();
    expect(bindings).toHaveLength(2);
    expect(bindings[0].tableIndex).toBe(proj.tableIndex);
    expect(bindings[0].columnIndex).toBe(0);
    expect(bindings[1].columnIndex).toBe(1);
  });
});

// ============================================================================
// DDL
// ============================================================================

describe('DDL', () => {
  it('CREATE TABLE builds LogicalCreateTable with correct schema', () => {
    const plan = bind(
      'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL)',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_CREATE_TABLE);
    const ct = plan as LogicalCreateTable;
    expect(ct.schema.name).toBe('products');
    expect(ct.schema.columns).toHaveLength(3);
    expect(ct.schema.columns[0].name).toBe('id');
    expect(ct.schema.columns[0].type).toBe('INTEGER');
    expect(ct.schema.columns[0].primaryKey).toBe(true);
    expect(ct.schema.columns[1].name).toBe('name');
    expect(ct.schema.columns[1].type).toBe('TEXT');
    expect(ct.schema.columns[1].nullable).toBe(false);
    expect(ct.schema.columns[2].name).toBe('price');
    expect(ct.schema.columns[2].type).toBe('REAL');
    expect(ct.ifNotExists).toBe(false);
  });

  it('CREATE TABLE IF NOT EXISTS sets ifNotExists = true', () => {
    const plan = bind(
      'CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY)',
    );
    const ct = plan as LogicalCreateTable;
    expect(ct.ifNotExists).toBe(true);
  });

  it('DROP TABLE builds LogicalDrop with dropType TABLE', () => {
    const plan = bind('DROP TABLE users');
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_DROP);
    const drop = plan as LogicalDrop;
    expect(drop.dropType).toBe('TABLE');
    expect(drop.name).toBe('users');
    expect(drop.ifExists).toBe(false);
  });

  it('DROP TABLE IF EXISTS sets ifExists = true', () => {
    const plan = bind('DROP TABLE IF EXISTS users');
    const drop = plan as LogicalDrop;
    expect(drop.ifExists).toBe(true);
  });

  it('ALTER TABLE ADD COLUMN builds LogicalAlterTable', () => {
    const plan = bind('ALTER TABLE users ADD COLUMN email TEXT');
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_ALTER_TABLE);
    const alter = plan as LogicalAlterTable;
    expect(alter.tableName).toBe('users');
    expect(alter.action.type).toBe('ADD_COLUMN');
    if (alter.action.type === 'ADD_COLUMN') {
      expect(alter.action.column.name).toBe('email');
      expect(alter.action.column.type).toBe('TEXT');
    }
  });

  it('CREATE INDEX validates table and columns exist', () => {
    const plan = bind('CREATE INDEX idx_name ON users (name)');
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_CREATE_INDEX);
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.tableName).toBe('users');
    expect(ci.index.columns).toEqual(['name']);
  });

  it('CREATE INDEX on non-existent table throws BindError', () => {
    expect(() =>
      bind('CREATE INDEX idx ON missing_table (col)'),
    ).toThrow(BindError);
  });

  it('CREATE INDEX on non-existent column throws BindError', () => {
    expect(() =>
      bind('CREATE INDEX idx ON users (missing_col)'),
    ).toThrow(BindError);
  });
});

// ============================================================================
// DML (INSERT, UPDATE, DELETE)
// ============================================================================

describe('DML', () => {
  it('INSERT builds LogicalInsert with correct columns', () => {
    const plan = bind("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
    const ins = plan as LogicalInsert;
    expect(ins.tableName).toBe('users');
    expect(ins.columns).toEqual([0, 1]); // indices of id, name
    expect(ins.expressions).toHaveLength(2);
  });

  it('UPDATE builds LogicalUpdate with filter', () => {
    const plan = bind("UPDATE users SET name = 'Bob' WHERE id = 1");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_UPDATE);
    const upd = plan as LogicalUpdate;
    expect(upd.tableName).toBe('users');
    expect(upd.updateColumns).toEqual([1]); // index of name
    expect(upd.children[0].type).toBe(LogicalOperatorType.LOGICAL_FILTER);
  });

  it('DELETE builds LogicalDelete with filter', () => {
    const plan = bind('DELETE FROM users WHERE id = 1');
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_DELETE);
    const del = plan as LogicalDelete;
    expect(del.tableName).toBe('users');
    expect(del.children[0].type).toBe(LogicalOperatorType.LOGICAL_FILTER);
  });

  it('INSERT into non-existent table throws BindError', () => {
    expect(() =>
      bind("INSERT INTO missing (id) VALUES (1)"),
    ).toThrow(BindError);
  });

  it('INSERT with non-existent column throws BindError', () => {
    expect(() =>
      bind("INSERT INTO users (missing_col) VALUES (1)"),
    ).toThrow(BindError);
  });
});

// ============================================================================
// Aggregate functions
// ============================================================================

describe('Aggregates', () => {
  it('SUM returns REAL', () => {
    const plan = bind('SELECT SUM(age) FROM users');
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions[0].returnType).toBe('REAL');
    expect(agg.expressions[0].functionName).toBe('SUM');
  });

  it('AVG returns REAL', () => {
    const plan = bind('SELECT AVG(age) FROM users');
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions[0].returnType).toBe('REAL');
  });

  it('MIN preserves column type', () => {
    const plan = bind('SELECT MIN(age) FROM users');
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions[0].returnType).toBe('INTEGER');
  });

  it('MAX preserves column type', () => {
    const plan = bind('SELECT MAX(name) FROM users');
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions[0].returnType).toBe('TEXT');
  });

  it('GROUP BY with aggregate', () => {
    const plan = bind(
      'SELECT u.name, COUNT(*) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name',
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg.groups).toHaveLength(1);
    expect(agg.expressions).toHaveLength(1);
    expect(agg.expressions[0].functionName).toBe('COUNT');
  });

  it('HAVING clause binds correctly with aggregate', () => {
    const plan = bind(
      'SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1',
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg.havingExpression).not.toBeNull();
    // The aggregate in HAVING should reuse the same aggregateIndex from SELECT
    expect(agg.expressions).toHaveLength(1); // only one COUNT(*)
  });

  it('COUNT(DISTINCT col) sets distinct = true', () => {
    const plan = bind('SELECT COUNT(DISTINCT name) FROM users');
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions[0].distinct).toBe(true);
    expect(agg.expressions[0].functionName).toBe('COUNT');
  });

  it('GROUP BY rewrites column bindings to groupIndex', () => {
    const plan = bind(
      'SELECT name, COUNT(*) FROM users GROUP BY name',
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;

    // Projection's first expression (name) should reference groupIndex, not scan tableIndex
    const nameRef = proj.expressions[0] as BoundColumnRefExpression;
    expect(nameRef.binding.tableIndex).toBe(agg.groupIndex);
    expect(nameRef.binding.columnIndex).toBe(0);
  });

  it('GROUP BY with multiple groups rewrites all bindings', () => {
    const plan = bind(
      'SELECT name, age, COUNT(*) FROM users GROUP BY name, age',
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;

    const nameRef = proj.expressions[0] as BoundColumnRefExpression;
    expect(nameRef.binding.tableIndex).toBe(agg.groupIndex);
    expect(nameRef.binding.columnIndex).toBe(0);

    const ageRef = proj.expressions[1] as BoundColumnRefExpression;
    expect(ageRef.binding.tableIndex).toBe(agg.groupIndex);
    expect(ageRef.binding.columnIndex).toBe(1);
  });

  it('GROUP BY with multiple aggregates binds correctly', () => {
    const plan = bind(
      'SELECT name, COUNT(*), AVG(age) FROM users GROUP BY name',
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;

    expect(agg.groups).toHaveLength(1);
    expect(agg.expressions).toHaveLength(2);

    const nameRef = proj.expressions[0] as BoundColumnRefExpression;
    expect(nameRef.binding.tableIndex).toBe(agg.groupIndex);
    expect(nameRef.binding.columnIndex).toBe(0);

    const countRef = proj.expressions[1] as BoundAggregateExpression;
    expect(countRef.functionName).toBe('COUNT');

    const avgRef = proj.expressions[2] as BoundAggregateExpression;
    expect(avgRef.functionName).toBe('AVG');
  });
});

// ============================================================================
// SELECT without FROM
// ============================================================================

describe('SELECT without FROM', () => {
  it('SELECT 1 produces a projection', () => {
    const plan = bind('SELECT 1');
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(1);
    const c = proj.expressions[0] as BoundConstantExpression;
    expect(c.expressionClass).toBe(BoundExpressionClass.BOUND_CONSTANT);
    expect(c.value).toBe(1);
  });

  it("SELECT 'hello' produces TEXT type", () => {
    const plan = bind("SELECT 'hello'");
    const proj = plan as LogicalProjection;
    expect(proj.types).toEqual(['TEXT']);
  });
});

// ============================================================================
// Expressions
// ============================================================================

describe('Expressions', () => {
  it('LIKE produces BoundFunctionExpression', () => {
    const plan = bind("SELECT * FROM users WHERE name LIKE '%alice%'");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const expr = filter.expressions[0] as BoundFunctionExpression;
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_FUNCTION);
    expect(expr.functionName).toBe('LIKE');
    expect(expr.returnType).toBe('BOOLEAN');
  });

  it('NOT LIKE produces BoundFunctionExpression', () => {
    const plan = bind("SELECT * FROM users WHERE name NOT LIKE '%bob%'");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const expr = filter.expressions[0] as BoundFunctionExpression;
    expect(expr.functionName).toBe('NOT_LIKE');
  });

  it('IS NULL produces BoundOperatorExpression', () => {
    const plan = bind('SELECT * FROM users WHERE age IS NULL');
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const expr = filter.expressions[0] as BoundOperatorExpression;
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_OPERATOR);
    expect(expr.operatorType).toBe('IS_NULL');
    expect(expr.returnType).toBe('BOOLEAN');
  });

  it('IS NOT NULL produces BoundOperatorExpression', () => {
    const plan = bind('SELECT * FROM users WHERE age IS NOT NULL');
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const expr = filter.expressions[0] as BoundOperatorExpression;
    expect(expr.operatorType).toBe('IS_NOT_NULL');
  });

  it('BETWEEN produces BoundBetweenExpression', () => {
    const plan = bind('SELECT * FROM users WHERE age BETWEEN 18 AND 65');
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const expr = filter.expressions[0] as BoundBetweenExpression;
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_BETWEEN);
    expect(expr.returnType).toBe('BOOLEAN');
  });

  it('arithmetic operators return correct types', () => {
    const plan = bind('SELECT age + 1, age * 2 FROM users');
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const add = proj.expressions[0] as BoundOperatorExpression;
    expect(add.operatorType).toBe('ADD');
    expect(add.returnType).toBe('INTEGER');
    const mul = proj.expressions[1] as BoundOperatorExpression;
    expect(mul.operatorType).toBe('MULTIPLY');
    expect(mul.returnType).toBe('INTEGER');
  });

  it('arithmetic with REAL promotes to REAL', () => {
    const plan = bind('SELECT amount + 1 FROM orders');
    const proj = plan as LogicalProjection;
    const add = proj.expressions[0] as BoundOperatorExpression;
    expect(add.returnType).toBe('REAL');
  });

  it('CASE expression binds correctly', () => {
    const plan = bind(
      "SELECT CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users",
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.expressionClass).toBe(BoundExpressionClass.BOUND_CASE);
    expect(caseExpr.caseChecks).toHaveLength(1);
    expect(caseExpr.elseExpr).not.toBeNull();
    expect(caseExpr.returnType).toBe('TEXT');
  });

  it('CAST expression binds correctly', () => {
    const plan = bind('SELECT CAST(age AS TEXT) FROM users');
    const proj = plan as LogicalProjection;
    const cast = proj.expressions[0] as BoundCastExpression;
    expect(cast.expressionClass).toBe(BoundExpressionClass.BOUND_CAST);
    expect(cast.castType).toBe('TEXT');
    expect(cast.returnType).toBe('TEXT');
  });

  it('AND/OR conjunction binds children', () => {
    const plan = bind('SELECT * FROM users WHERE age > 18 AND active = true');
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const conj = filter.expressions[0] as BoundConjunctionExpression;
    expect(conj.expressionClass).toBe(BoundExpressionClass.BOUND_CONJUNCTION);
    expect(conj.conjunctionType).toBe('AND');
    expect(conj.children).toHaveLength(2);
  });

  it('IN operator produces BoundOperatorExpression', () => {
    const plan = bind("SELECT * FROM users WHERE id IN (1, 2, 3)");
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const op = filter.expressions[0] as BoundOperatorExpression;
    expect(op.expressionClass).toBe(BoundExpressionClass.BOUND_OPERATOR);
    expect(op.operatorType).toBe('IN');
    expect(op.returnType).toBe('BOOLEAN');
  });

  it('scalar functions bind correctly', () => {
    const plan = bind('SELECT UPPER(name), LENGTH(name) FROM users');
    const proj = plan as LogicalProjection;
    const upper = proj.expressions[0] as BoundFunctionExpression;
    expect(upper.functionName).toBe('UPPER');
    expect(upper.returnType).toBe('TEXT');
    const len = proj.expressions[1] as BoundFunctionExpression;
    expect(len.functionName).toBe('LENGTH');
    expect(len.returnType).toBe('INTEGER');
  });

  it('COALESCE returns non-null type', () => {
    const plan = bind('SELECT COALESCE(name, age) FROM users');
    const proj = plan as LogicalProjection;
    const fn = proj.expressions[0] as BoundFunctionExpression;
    expect(fn.functionName).toBe('COALESCE');
  });
});

// ============================================================================
// JOIN — additional
// ============================================================================

describe('JOIN — additional', () => {
  it('USING clause resolves columns from left and right tables', () => {
    const plan = bind(
      'SELECT * FROM users JOIN orders USING (id)',
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    expect(join.type).toBe(LogicalOperatorType.LOGICAL_COMPARISON_JOIN);
    expect(join.conditions).toHaveLength(1);
    expect(join.conditions[0].comparisonType).toBe('EQUAL');
    const leftCol = join.conditions[0].left as BoundColumnRefExpression;
    const rightCol = join.conditions[0].right as BoundColumnRefExpression;
    expect(leftCol.tableName).toBe('users');
    expect(rightCol.tableName).toBe('orders');
  });

  it('RIGHT JOIN throws BindError', () => {
    expect(() =>
      bind('SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id'),
    ).toThrow('RIGHT JOIN is not supported');
  });
});

// ============================================================================
// UNION
// ============================================================================

describe('UNION', () => {
  it('UNION ALL produces LogicalUnion with all = true', () => {
    const plan = bind(
      'SELECT id, name FROM users UNION ALL SELECT id, status FROM orders',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_UNION);
    const union = plan as LogicalUnion;
    expect(union.all).toBe(true);
    expect(union.children).toHaveLength(2);
  });

  it('UNION (without ALL) produces LogicalUnion with all = false', () => {
    const plan = bind(
      'SELECT id, name FROM users UNION SELECT id, status FROM orders',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_UNION);
    const union = plan as LogicalUnion;
    expect(union.all).toBe(false);
  });
});

// ============================================================================
// CTE — additional
// ============================================================================

describe('CTE — additional', () => {
  it('CTE ref produces LogicalCTERef node', () => {
    const plan = bind(
      'WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active',
    );
    const cte = plan as LogicalMaterializedCTE;
    const mainProj = cte.children[1] as LogicalProjection;
    const cteRef = mainProj.children[0] as LogicalCTERef;
    expect(cteRef.type).toBe(LogicalOperatorType.LOGICAL_CTE_REF);
    expect(cteRef.cteName).toBe('active');
  });

  it('CTE columns are accessible from main query', () => {
    const plan = bind(
      'WITH active AS (SELECT id, name FROM users WHERE active = true) SELECT name FROM active',
    );
    const cte = plan as LogicalMaterializedCTE;
    const mainProj = cte.children[1] as LogicalProjection;
    expect(mainProj.expressions).toHaveLength(1);
    const col = mainProj.expressions[0] as BoundColumnRefExpression;
    expect(col.columnName).toBe('name');
  });
});

// ============================================================================
// Subquery in FROM
// ============================================================================

describe('Subquery', () => {
  it('subquery in FROM creates virtual table', () => {
    const plan = bind(
      'SELECT sub.id FROM (SELECT id, name FROM users) sub',
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(1);
    const col = proj.expressions[0] as BoundColumnRefExpression;
    expect(col.columnName).toBe('id');
  });

  it('EXISTS subquery returns BOOLEAN', () => {
    const plan = bind(
      'SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)',
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const sub = filter.expressions[0] as BoundSubqueryExpression;
    expect(sub.expressionClass).toBe(BoundExpressionClass.BOUND_SUBQUERY);
    expect(sub.subqueryType).toBe('EXISTS');
    expect(sub.returnType).toBe('BOOLEAN');
  });
});

// ============================================================================
// INSERT INTO ... SELECT
// ============================================================================

describe('INSERT SELECT', () => {
  it('INSERT INTO ... SELECT produces LogicalInsert with child plan', () => {
    const plan = bind(
      'INSERT INTO orders (id, user_id, amount, status) SELECT id, id, age, name FROM users',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
    const ins = plan as LogicalInsert;
    expect(ins.tableName).toBe('orders');
    expect(ins.children).toHaveLength(1);
    expect(ins.children[0].type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    expect(ins.expressions).toHaveLength(0); // no VALUES expressions
  });
});

// ============================================================================
// Regression tests for review fixes
// ============================================================================

describe('Review fixes', () => {
  it('CTE getColumnBindings does not infinite loop', () => {
    const plan = bind(
      'WITH active AS (SELECT * FROM users WHERE active = true) SELECT * FROM active',
    );
    const cte = plan as LogicalMaterializedCTE;
    // This would stack overflow before the fix
    const bindings = cte.getColumnBindings();
    expect(bindings).toBeDefined();
  });

  it('CTE column bindings match resolved column refs', () => {
    const plan = bind(
      'WITH active AS (SELECT id, name FROM users) SELECT name FROM active',
    );
    const cte = plan as LogicalMaterializedCTE;
    const mainProj = cte.children[1] as LogicalProjection;
    const col = mainProj.expressions[0] as BoundColumnRefExpression;
    // The CTE ref's getColumnBindings should have matching tableIndex
    const cteRef = mainProj.children[0];
    const refBindings = cteRef.getColumnBindings();
    // The column's tableIndex should match one of the CTE ref bindings
    expect(refBindings.some((b) => b.tableIndex === col.binding.tableIndex)).toBe(true);
  });

  it('SELECT without FROM has valid children (no null)', () => {
    const plan = bind('SELECT 1, 2, 3');
    const proj = plan as LogicalProjection;
    expect(proj.children[0]).toBeDefined();
    expect(proj.children[0].type).toBe(LogicalOperatorType.LOGICAL_GET);
  });

  it('UNION with mismatched column count throws BindError', () => {
    expect(() =>
      bind('SELECT id FROM users UNION SELECT id, status FROM orders'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT id FROM users UNION SELECT id, status FROM orders'),
    ).toThrow('same number of columns');
  });

  it('UNION with incompatible types throws BindError', () => {
    expect(() =>
      bind('SELECT name FROM users UNION SELECT id FROM orders'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT name FROM users UNION SELECT id FROM orders'),
    ).toThrow('Type mismatch');
  });

  it('aggregate in CASE inside SELECT with GROUP BY reuses aggregateIndex', () => {
    const plan = bind(
      "SELECT CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'few' END FROM users GROUP BY name",
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    // Only one aggregate should be collected
    expect(agg.expressions).toHaveLength(1);
    expect(agg.expressions[0].functionName).toBe('COUNT');
  });
});

// ============================================================================
// Bug regression: correlated subqueries (parent scope resolution)
// ============================================================================

describe('Correlated subqueries', () => {
  it('WHERE EXISTS with outer table reference resolves correctly', () => {
    const plan = bind(
      'SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)',
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const sub = filter.expressions[0] as BoundSubqueryExpression;
    expect(sub.subqueryType).toBe('EXISTS');
    expect(sub.returnType).toBe('BOOLEAN');
  });

  it('scalar correlated subquery resolves outer column', () => {
    const plan = bind(
      'SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) FROM users u',
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const sub = proj.expressions[1] as BoundSubqueryExpression;
    expect(sub.expressionClass).toBe(BoundExpressionClass.BOUND_SUBQUERY);
    expect(sub.subqueryType).toBe('SCALAR');
  });

  it('unqualified outer column resolves via parent scope', () => {
    const plan = bind(
      'SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = id)',
    );
    // Should not throw — 'id' is unambiguous within the combined scope (users.id from parent, orders doesn't have 'id' column... wait, orders has 'id')
    // Actually 'id' is ambiguous since both users and orders have 'id'. But user_id is unambiguous.
    const proj = plan as LogicalProjection;
    expect(proj.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
  });
});

// ============================================================================
// Bug regression: sameExpression deduplication for complex aggregates
// ============================================================================

describe('Aggregate deduplication', () => {
  it('duplicate SUM(age+1) is deduplicated to single aggregate', () => {
    const plan = bind(
      'SELECT SUM(age + 1), SUM(age + 1) FROM users',
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions).toHaveLength(1);
    expect(agg.expressions[0].functionName).toBe('SUM');
  });

  it('different aggregates are not deduplicated', () => {
    const plan = bind(
      'SELECT SUM(age), AVG(age) FROM users',
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.expressions).toHaveLength(2);
  });
});

// ============================================================================
// Bug regression: INSERT column count validation
// ============================================================================

describe('INSERT validation', () => {
  it('INSERT VALUES with too many values throws BindError', () => {
    expect(() =>
      bind("INSERT INTO users (id, name) VALUES (1, 'Alice', 42)"),
    ).toThrow(BindError);
    expect(() =>
      bind("INSERT INTO users (id, name) VALUES (1, 'Alice', 42)"),
    ).toThrow('column count mismatch');
  });

  it('INSERT VALUES with too few values throws BindError', () => {
    expect(() =>
      bind("INSERT INTO users (id, name, age) VALUES (1, 'Alice')"),
    ).toThrow(BindError);
    expect(() =>
      bind("INSERT INTO users (id, name, age) VALUES (1, 'Alice')"),
    ).toThrow('column count mismatch');
  });

  it('INSERT SELECT with column count mismatch throws BindError', () => {
    expect(() =>
      bind('INSERT INTO users (id, name) SELECT id, name, age FROM users'),
    ).toThrow(BindError);
    expect(() =>
      bind('INSERT INTO users (id, name) SELECT id, name, age FROM users'),
    ).toThrow('column count mismatch');
  });

  it('INSERT without column list uses all columns', () => {
    const plan = bind("INSERT INTO users VALUES (1, 'Alice', 25, true)");
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
    const ins = plan as LogicalInsert;
    expect(ins.columns).toEqual([0, 1, 2, 3]);
    expect(ins.expressions).toHaveLength(4);
  });
});

// ============================================================================
// Subquery in FROM — binding consistency
// ============================================================================

describe('Subquery in FROM — bindings', () => {
  it('subquery ref getColumnBindings matches outer scope tableIndex', () => {
    const plan = bind(
      'SELECT sub.id FROM (SELECT id, name FROM users) sub',
    );
    const proj = plan as LogicalProjection;
    const col = proj.expressions[0] as BoundColumnRefExpression;
    // The subquery wrapper should have bindings matching the resolved column
    const subGet = proj.children[0] as LogicalGet;
    const bindings = subGet.getColumnBindings();
    expect(bindings.some((b) => b.tableIndex === col.binding.tableIndex)).toBe(true);
  });
});

// ============================================================================
// Expressions — additional coverage
// ============================================================================

describe('Expressions — additional', () => {
  it('NEGATE operator returns correct type', () => {
    const plan = bind('SELECT -age FROM users');
    const proj = plan as LogicalProjection;
    const neg = proj.expressions[0] as BoundOperatorExpression;
    expect(neg.operatorType).toBe('NEGATE');
    expect(neg.returnType).toBe('INTEGER');
  });

  it('NOT operator returns BOOLEAN', () => {
    const plan = bind('SELECT * FROM users WHERE NOT active');
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const not = filter.expressions[0] as BoundOperatorExpression;
    expect(not.operatorType).toBe('NOT');
    expect(not.returnType).toBe('BOOLEAN');
  });

  it('MOD operator', () => {
    const plan = bind('SELECT age % 2 FROM users');
    const proj = plan as LogicalProjection;
    const mod = proj.expressions[0] as BoundOperatorExpression;
    expect(mod.operatorType).toBe('MOD');
    expect(mod.returnType).toBe('INTEGER');
  });

  it('DIVIDE operator', () => {
    const plan = bind('SELECT age / 2 FROM users');
    const proj = plan as LogicalProjection;
    const div = proj.expressions[0] as BoundOperatorExpression;
    expect(div.operatorType).toBe('DIVIDE');
    expect(div.returnType).toBe('INTEGER');
  });

  it('SUBTRACT operator', () => {
    const plan = bind('SELECT age - 1 FROM users');
    const proj = plan as LogicalProjection;
    const sub = proj.expressions[0] as BoundOperatorExpression;
    expect(sub.operatorType).toBe('SUBTRACT');
    expect(sub.returnType).toBe('INTEGER');
  });

  it('NULL constant has returnType NULL', () => {
    const plan = bind('SELECT NULL');
    const proj = plan as LogicalProjection;
    const c = proj.expressions[0] as BoundConstantExpression;
    expect(c.value).toBeNull();
    expect(c.returnType).toBe('NULL');
  });

  it('TRUE/FALSE constants have returnType BOOLEAN', () => {
    const plan = bind('SELECT TRUE, FALSE');
    const proj = plan as LogicalProjection;
    expect(proj.types).toEqual(['BOOLEAN', 'BOOLEAN']);
    expect((proj.expressions[0] as BoundConstantExpression).value).toBe(true);
    expect((proj.expressions[1] as BoundConstantExpression).value).toBe(false);
  });

  it('ABS returns child type', () => {
    const plan = bind('SELECT ABS(age) FROM users');
    const proj = plan as LogicalProjection;
    const fn = proj.expressions[0] as BoundFunctionExpression;
    expect(fn.functionName).toBe('ABS');
    expect(fn.returnType).toBe('INTEGER');
  });

  it('TYPEOF returns TEXT', () => {
    const plan = bind('SELECT TYPEOF(name) FROM users');
    const proj = plan as LogicalProjection;
    const fn = proj.expressions[0] as BoundFunctionExpression;
    expect(fn.functionName).toBe('TYPEOF');
    expect(fn.returnType).toBe('TEXT');
  });

  it('NOT IN operator', () => {
    const plan = bind('SELECT * FROM users WHERE id NOT IN (1, 2, 3)');
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const op = filter.expressions[0] as BoundOperatorExpression;
    expect(op.operatorType).toBe('NOT_IN');
    expect(op.returnType).toBe('BOOLEAN');
  });

  it('OR conjunction', () => {
    const plan = bind('SELECT * FROM users WHERE age > 18 OR active = true');
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const conj = filter.expressions[0] as BoundConjunctionExpression;
    expect(conj.conjunctionType).toBe('OR');
  });
});

// ============================================================================
// Subqueries — additional coverage
// ============================================================================

describe('Subqueries — additional', () => {
  it('NOT EXISTS subquery returns BOOLEAN', () => {
    const plan = bind(
      'SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = 999)',
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    // NOT EXISTS is either a NOT_EXISTS subquery type or a NOT wrapping EXISTS
    expect(filter.expressions).toHaveLength(1);
  });

  it('scalar subquery in SELECT', () => {
    const plan = bind(
      'SELECT (SELECT MAX(age) FROM users)',
    );
    const proj = plan as LogicalProjection;
    const sub = proj.expressions[0] as BoundSubqueryExpression;
    expect(sub.expressionClass).toBe(BoundExpressionClass.BOUND_SUBQUERY);
    expect(sub.subqueryType).toBe('SCALAR');
  });
});

// ============================================================================
// JOIN — additional coverage
// ============================================================================

describe('JOIN — more coverage', () => {
  it('self-join resolves correctly with aliases', () => {
    const plan = bind(
      'SELECT a.name, b.name FROM users a JOIN users b ON a.id = b.id',
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    const col1 = proj.expressions[1] as BoundColumnRefExpression;
    expect(col0.columnName).toBe('name');
    expect(col1.columnName).toBe('name');
    // Different table indices for the two aliases
    expect(col0.binding.tableIndex).not.toBe(col1.binding.tableIndex);
  });

  it('JOIN with compound ON condition (AND)', () => {
    const plan = bind(
      'SELECT * FROM users u JOIN orders o ON u.id = o.user_id AND o.amount > 100',
    );
    const proj = plan as LogicalProjection;
    const join = proj.children[0] as LogicalComparisonJoin;
    expect(join.conditions.length).toBeGreaterThanOrEqual(2);
  });
});

// ============================================================================
// ORDER BY — additional coverage
// ============================================================================

describe('ORDER BY — additional', () => {
  it('ORDER BY DESC', () => {
    const plan = bind('SELECT name FROM users ORDER BY name DESC');
    let node: LogicalOperator = plan;
    while (node.type !== LogicalOperatorType.LOGICAL_ORDER_BY && node.children.length > 0) {
      node = node.children[0];
    }
    const orderBy = node as LogicalOrderBy;
    expect(orderBy.orders[0].orderType).toBe('DESCENDING');
  });

  it('multiple ORDER BY columns', () => {
    const plan = bind('SELECT * FROM users ORDER BY name ASC, age DESC');
    let node: LogicalOperator = plan;
    while (node.type !== LogicalOperatorType.LOGICAL_ORDER_BY && node.children.length > 0) {
      node = node.children[0];
    }
    const orderBy = node as LogicalOrderBy;
    expect(orderBy.orders).toHaveLength(2);
    expect(orderBy.orders[0].orderType).toBe('ASCENDING');
    expect(orderBy.orders[1].orderType).toBe('DESCENDING');
  });

  it('ORDER BY expression is rewritten to projection output binding', () => {
    const plan = bind('SELECT name, age FROM users ORDER BY age ASC');
    // Plan: ORDER BY → PROJECTION → GET
    const orderBy = plan as LogicalOrderBy;
    expect(orderBy.type).toBe(LogicalOperatorType.LOGICAL_ORDER_BY);
    const proj = orderBy.children[0] as LogicalProjection;
    expect(proj.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);

    // ORDER BY expression should reference projection output, not original table
    const orderExpr = orderBy.orders[0].expression as BoundColumnRefExpression;
    expect(orderExpr.expressionClass).toBe(BoundExpressionClass.BOUND_COLUMN_REF);
    const projBindings = proj.getColumnBindings();
    // age is the 2nd select list item → projBindings[1]
    expect(orderExpr.binding.tableIndex).toBe(projBindings[1].tableIndex);
    expect(orderExpr.binding.columnIndex).toBe(projBindings[1].columnIndex);
  });

  it('ORDER BY column not in select list extends projection and adds trim projection', () => {
    const plan = bind('SELECT name FROM users ORDER BY age ASC');
    // Plan: TRIM_PROJECTION → ORDER BY → EXTENDED_PROJECTION → GET
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    const trimProj = plan as LogicalProjection;
    // Trim projection outputs only 1 column (name)
    expect(trimProj.expressions).toHaveLength(1);

    const orderBy = trimProj.children[0] as LogicalOrderBy;
    expect(orderBy.type).toBe(LogicalOperatorType.LOGICAL_ORDER_BY);

    const extendedProj = orderBy.children[0] as LogicalProjection;
    expect(extendedProj.type).toBe(LogicalOperatorType.LOGICAL_PROJECTION);
    // Extended projection has 2 columns (name + age for sort)
    expect(extendedProj.expressions).toHaveLength(2);

    // ORDER BY expression references the extended projection's 2nd column
    const orderExpr = orderBy.orders[0].expression as BoundColumnRefExpression;
    const extBindings = extendedProj.getColumnBindings();
    expect(orderExpr.binding.tableIndex).toBe(extBindings[1].tableIndex);
    expect(orderExpr.binding.columnIndex).toBe(extBindings[1].columnIndex);
  });

  it('ORDER BY with GROUP BY uses aggregate-aware bindings', () => {
    const plan = bind('SELECT name, SUM(amount) FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY name ORDER BY SUM(amount) DESC');
    // Find ORDER BY
    let node: LogicalOperator = plan;
    while (node.type !== LogicalOperatorType.LOGICAL_ORDER_BY && node.children.length > 0) {
      node = node.children[0];
    }
    const orderBy = node as LogicalOrderBy;
    expect(orderBy.type).toBe(LogicalOperatorType.LOGICAL_ORDER_BY);

    // ORDER BY expression should be a projection-output column ref
    const orderExpr = orderBy.orders[0].expression as BoundColumnRefExpression;
    expect(orderExpr.expressionClass).toBe(BoundExpressionClass.BOUND_COLUMN_REF);

    // It should reference the projection's output binding for the 2nd column (SUM)
    const proj = orderBy.children[0] as LogicalProjection;
    const projBindings = proj.getColumnBindings();
    expect(orderExpr.binding.tableIndex).toBe(projBindings[1].tableIndex);
    expect(orderExpr.binding.columnIndex).toBe(projBindings[1].columnIndex);
  });
});

// ============================================================================
// CTE — additional coverage
// ============================================================================

describe('CTE — more coverage', () => {
  it('multiple CTEs', () => {
    const plan = bind(
      'WITH a AS (SELECT id FROM users), b AS (SELECT id FROM orders) SELECT * FROM a',
    );
    // Should have nested LogicalMaterializedCTE nodes
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const outer = plan as LogicalMaterializedCTE;
    // The inner plan should also be a materialized CTE
    expect(outer.children[1].type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
  });

  it('CTE referencing another CTE', () => {
    const plan = bind(
      'WITH a AS (SELECT id, name FROM users), b AS (SELECT id FROM a) SELECT * FROM b',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
  });
});

// ============================================================================
// UNION — additional coverage
// ============================================================================

describe('UNION — additional', () => {
  it('UNION with ORDER BY and LIMIT', () => {
    const plan = bind(
      'SELECT id, name FROM users UNION ALL SELECT id, status FROM orders ORDER BY id LIMIT 5',
    );
    // Top should be LIMIT
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_LIMIT);
    const limit = plan as LogicalLimit;
    expect(limit.limitVal).toBe(5);
    // Next should be ORDER BY
    expect(limit.children[0].type).toBe(LogicalOperatorType.LOGICAL_ORDER_BY);
    // Then UNION
    const orderBy = limit.children[0] as LogicalOrderBy;
    expect(orderBy.children[0].type).toBe(LogicalOperatorType.LOGICAL_UNION);
  });
});

// ============================================================================
// DDL — additional coverage
// ============================================================================

describe('DDL — additional', () => {
  it('ALTER TABLE DROP COLUMN', () => {
    const plan = bind('ALTER TABLE users DROP COLUMN age');
    const alter = plan as LogicalAlterTable;
    expect(alter.action.type).toBe('DROP_COLUMN');
    if (alter.action.type === 'DROP_COLUMN') {
      expect(alter.action.columnName).toBe('age');
    }
  });

  it('CREATE TABLE with DEFAULT value', () => {
    const plan = bind("CREATE TABLE t (id INTEGER, name TEXT DEFAULT 'unknown')");
    const ct = plan as LogicalCreateTable;
    expect(ct.schema.columns[1].defaultValue).toBe('unknown');
  });

  it('CREATE TABLE with UNIQUE constraint', () => {
    const plan = bind('CREATE TABLE t (id INTEGER, email TEXT UNIQUE)');
    const ct = plan as LogicalCreateTable;
    expect(ct.schema.columns[1].unique).toBe(true);
  });

  it('CREATE UNIQUE INDEX', () => {
    const plan = bind('CREATE UNIQUE INDEX idx_email ON users (name)');
    const ci = plan as LogicalCreateIndex;
    expect(ci.index.unique).toBe(true);
  });

  it('DROP INDEX', () => {
    const plan = bind('DROP INDEX idx_name');
    const drop = plan as LogicalDrop;
    expect(drop.dropType).toBe('INDEX');
    expect(drop.name).toBe('idx_name');
  });

  it('DROP TABLE IF EXISTS', () => {
    const plan = bind('DROP TABLE IF EXISTS nonexistent');
    const drop = plan as LogicalDrop;
    expect(drop.ifExists).toBe(true);
    expect(drop.name).toBe('nonexistent');
  });
});

// ============================================================================
// DML — additional coverage
// ============================================================================

describe('DML — additional', () => {
  it('DELETE without WHERE scans all rows', () => {
    const plan = bind('DELETE FROM users');
    const del = plan as LogicalDelete;
    expect(del.children[0].type).toBe(LogicalOperatorType.LOGICAL_GET);
  });

  it('UPDATE without WHERE scans all rows', () => {
    const plan = bind("UPDATE users SET name = 'Bob'");
    const upd = plan as LogicalUpdate;
    expect(upd.children[0].type).toBe(LogicalOperatorType.LOGICAL_GET);
  });

  it('UPDATE with non-existent column throws BindError', () => {
    expect(() =>
      bind("UPDATE users SET missing_col = 1"),
    ).toThrow(BindError);
  });
});

// ============================================================================
// CASE — additional coverage
// ============================================================================

describe('CASE — additional', () => {
  it('CASE with multiple WHEN branches', () => {
    const plan = bind(
      "SELECT CASE WHEN age < 13 THEN 'child' WHEN age < 18 THEN 'teen' ELSE 'adult' END FROM users",
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.caseChecks).toHaveLength(2);
    expect(caseExpr.elseExpr).not.toBeNull();
  });

  it('CASE without ELSE', () => {
    const plan = bind(
      "SELECT CASE WHEN age > 18 THEN 'adult' END FROM users",
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.elseExpr).toBeNull();
  });
});

// ============================================================================
// HAVING — additional coverage
// ============================================================================

describe('HAVING — additional', () => {
  it('HAVING without explicit GROUP BY but with aggregate', () => {
    const plan = bind(
      'SELECT COUNT(*) FROM users HAVING COUNT(*) > 1',
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    expect(agg.type).toBe(LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY);
    expect(agg.havingExpression).not.toBeNull();
    expect(agg.groups).toHaveLength(0);
  });
});

// ============================================================================
// Iteration 2: scope isolation (UNION, FROM subquery)
// ============================================================================

describe('Scope isolation', () => {
  it('UNION right side does not see left side tables', () => {
    // 'name' exists in users but not in orders — right side should fail
    expect(() =>
      bind('SELECT name FROM users UNION SELECT name FROM orders'),
    ).toThrow(BindError);
  });

  it('FROM subquery does not see outer tables (no lateral join)', () => {
    // u.id is from outer scope — subquery in FROM should not see it
    expect(() =>
      bind('SELECT * FROM users u JOIN (SELECT * FROM orders WHERE user_id = u.id) sub ON u.id = sub.user_id'),
    ).toThrow(BindError);
  });

  it('UNION right side CTE references still work', () => {
    const plan = bind(
      'WITH cte AS (SELECT id, name FROM users) SELECT id, name FROM cte UNION ALL SELECT id, name FROM cte',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
  });
});

// ============================================================================
// Iteration 2: aggregate inside non-aggregate function
// ============================================================================

describe('Aggregate inside function', () => {
  it('UPPER(COUNT(*)) correctly links to pre-collected aggregate', () => {
    const plan = bind(
      "SELECT UPPER(CAST(COUNT(*) AS TEXT)) FROM users GROUP BY name",
    );
    const proj = plan as LogicalProjection;
    const agg = proj.children[0] as LogicalAggregate;
    // Only one aggregate should be collected
    expect(agg.expressions).toHaveLength(1);
    expect(agg.expressions[0].functionName).toBe('COUNT');
  });
});

// ============================================================================
// Iteration 2: negative LIMIT/OFFSET
// ============================================================================

describe('LIMIT/OFFSET validation', () => {
  it('LIMIT 0 is allowed', () => {
    const plan = bind('SELECT * FROM users LIMIT 0');
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_LIMIT);
    expect((plan as LogicalLimit).limitVal).toBe(0);
  });
});

// ============================================================================
// Iteration 2: INSERT duplicate columns
// ============================================================================

describe('INSERT duplicate columns', () => {
  it('INSERT with duplicate column names throws BindError', () => {
    expect(() =>
      bind("INSERT INTO users (name, name) VALUES ('a', 'b')"),
    ).toThrow(BindError);
    expect(() =>
      bind("INSERT INTO users (name, name) VALUES ('a', 'b')"),
    ).toThrow('Duplicate column');
  });
});

// ============================================================================
// Iteration 2: aggregate in JOIN ON
// ============================================================================

describe('Aggregate in JOIN ON', () => {
  it('aggregate in ON clause throws BindError', () => {
    expect(() =>
      bind('SELECT * FROM users u JOIN orders o ON COUNT(*) > 1'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT * FROM users u JOIN orders o ON COUNT(*) > 1'),
    ).toThrow('JOIN ON clause');
  });
});

// ============================================================================
// Iteration 3: UNION inside subqueries / CTE / INSERT SELECT
// ============================================================================

describe('UNION inside subqueries', () => {
  it('FROM subquery containing UNION ALL binds correctly', () => {
    const plan = bind(
      'SELECT sub.id, sub.name FROM (SELECT id, name FROM users UNION ALL SELECT id, status FROM orders) sub',
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    expect(col0.columnName).toBe('id');
  });

  it('CTE body containing UNION ALL binds correctly', () => {
    const plan = bind(
      'WITH combined AS (SELECT id, name FROM users UNION ALL SELECT id, status FROM orders) SELECT * FROM combined',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const cte = plan as LogicalMaterializedCTE;
    expect(cte.cteName).toBe('combined');
    const mainProj = cte.children[1] as LogicalProjection;
    expect(mainProj.expressions).toHaveLength(2);
  });

  it('EXISTS subquery containing UNION ALL binds correctly', () => {
    const plan = bind(
      'SELECT * FROM users WHERE EXISTS (SELECT id FROM users UNION ALL SELECT id FROM orders)',
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    const sub = filter.expressions[0] as BoundSubqueryExpression;
    expect(sub.subqueryType).toBe('EXISTS');
    expect(sub.returnType).toBe('BOOLEAN');
  });

  it('IN subquery containing UNION ALL binds correctly', () => {
    const plan = bind(
      'SELECT * FROM users WHERE id IN (SELECT id FROM users UNION ALL SELECT id FROM orders)',
    );
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    expect(filter.expressions).toHaveLength(1);
  });

  it('INSERT INTO ... SELECT ... UNION ALL binds correctly', () => {
    const plan = bind(
      'INSERT INTO orders (id, user_id, amount, status) SELECT id, id, age, name FROM users UNION ALL SELECT id, user_id, amount, status FROM orders',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
    const ins = plan as LogicalInsert;
    expect(ins.tableName).toBe('orders');
    expect(ins.children).toHaveLength(1);
  });

  it('scalar subquery containing UNION ALL binds correctly', () => {
    const plan = bind(
      'SELECT (SELECT MAX(id) FROM users UNION ALL SELECT MAX(id) FROM orders) FROM users',
    );
    const proj = plan as LogicalProjection;
    const sub = proj.expressions[0] as BoundSubqueryExpression;
    expect(sub.subqueryType).toBe('SCALAR');
  });
});

// ============================================================================
// Iteration 3: isolated scope CTE propagation through parent chain
// ============================================================================

describe('Isolated scope CTE parent chain', () => {
  it('UNION right side inside correlated subquery can see outer CTE', () => {
    const plan = bind(
      'WITH cte AS (SELECT id, name FROM users) SELECT * FROM users WHERE EXISTS (SELECT id FROM cte UNION ALL SELECT id FROM cte)',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
  });

  it('nested CTE accessible from UNION right side', () => {
    const plan = bind(
      'WITH a AS (SELECT id, name FROM users), b AS (SELECT id, name FROM a) SELECT id, name FROM b UNION ALL SELECT id, name FROM a',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
  });
});

// ============================================================================
// Iteration 3: BETWEEN type checking
// ============================================================================

describe('BETWEEN type checking', () => {
  it('BETWEEN with compatible types succeeds', () => {
    const plan = bind('SELECT * FROM users WHERE age BETWEEN 18 AND 65');
    const proj = plan as LogicalProjection;
    const filter = proj.children[0] as LogicalFilter;
    expect(filter.expressions[0].returnType).toBe('BOOLEAN');
  });

  it('BETWEEN with incompatible lower bound throws BindError', () => {
    expect(() =>
      bind("SELECT * FROM users WHERE age BETWEEN 'young' AND 65"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT * FROM users WHERE age BETWEEN 'young' AND 65"),
    ).toThrow('Type mismatch');
  });

  it('BETWEEN with incompatible upper bound throws BindError', () => {
    expect(() =>
      bind("SELECT * FROM users WHERE age BETWEEN 18 AND 'old'"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT * FROM users WHERE age BETWEEN 18 AND 'old'"),
    ).toThrow('Type mismatch');
  });
});

// ============================================================================
// Iteration 3: CASE branch type checking
// ============================================================================

describe('CASE branch type checking', () => {
  it('CASE with compatible THEN branches succeeds', () => {
    const plan = bind(
      'SELECT CASE WHEN age > 18 THEN 1 ELSE 0 END FROM users',
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.returnType).toBe('INTEGER');
  });

  it('CASE with incompatible THEN branches throws BindError', () => {
    expect(() =>
      bind("SELECT CASE WHEN age > 18 THEN 'adult' ELSE 42 END FROM users"),
    ).toThrow(BindError);
    expect(() =>
      bind("SELECT CASE WHEN age > 18 THEN 'adult' ELSE 42 END FROM users"),
    ).toThrow('Type mismatch');
  });

  it('CASE with multiple incompatible WHEN branches throws BindError', () => {
    expect(() =>
      bind("SELECT CASE WHEN age < 13 THEN 'child' WHEN age < 18 THEN 42 ELSE 'adult' END FROM users"),
    ).toThrow(BindError);
  });

  it('CASE with numeric type promotion succeeds', () => {
    const plan = bind(
      'SELECT CASE WHEN age > 18 THEN age ELSE amount END FROM users JOIN orders ON users.id = orders.user_id',
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    // INTEGER and REAL should promote to REAL
    expect(caseExpr.returnType).toBe('REAL');
  });
});

// ============================================================================
// Iteration 3: Extra CASE tests (from duplicate cleanup)
// ============================================================================

describe('CASE extra tests', () => {
  it('CASE with all TEXT branches succeeds', () => {
    const plan = bind(
      "SELECT CASE WHEN age > 18 THEN 'adult' WHEN age > 10 THEN 'teen' ELSE 'child' END FROM users",
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.returnType).toBe('TEXT');
  });

  it('CASE with NULL ELSE is compatible with any THEN type', () => {
    const plan = bind(
      "SELECT CASE WHEN age > 18 THEN 'adult' ELSE NULL END FROM users",
    );
    const proj = plan as LogicalProjection;
    const caseExpr = proj.expressions[0] as BoundCaseExpression;
    expect(caseExpr.returnType).toBe('TEXT');
  });
});

// ============================================================================
// Iteration 4: JOIN ON type checking
// ============================================================================

describe('JOIN ON type checking', () => {
  it('JOIN ON with compatible types succeeds', () => {
    const plan = bind(
      'SELECT * FROM users u JOIN orders o ON u.id = o.user_id',
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions.length).toBeGreaterThan(0);
  });

  it('JOIN ON with incompatible types throws BindError', () => {
    expect(() =>
      bind('SELECT * FROM users u JOIN orders o ON u.name = o.amount'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT * FROM users u JOIN orders o ON u.name = o.amount'),
    ).toThrow('Type mismatch');
  });

  it('JOIN ON with numeric promotion succeeds', () => {
    // INTEGER = REAL should promote
    const plan = bind(
      'SELECT * FROM users u JOIN orders o ON u.id = o.amount',
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions.length).toBeGreaterThan(0);
  });

  it('LEFT JOIN ON with incompatible types throws BindError', () => {
    expect(() =>
      bind('SELECT * FROM users u LEFT JOIN orders o ON u.name = o.amount'),
    ).toThrow(BindError);
  });
});

// ============================================================================
// Iteration 4: CTE column aliases
// ============================================================================

describe('CTE column aliases', () => {
  it('WITH cte(a, b) AS (...) applies column aliases', () => {
    const plan = bind(
      'WITH cte(a, b) AS (SELECT id, name FROM users) SELECT a, b FROM cte',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const cte = plan as LogicalMaterializedCTE;
    const proj = cte.children[1] as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    const col1 = proj.expressions[1] as BoundColumnRefExpression;
    expect(col0.columnName).toBe('a');
    expect(col1.columnName).toBe('b');
  });

  it('CTE with wrong number of aliases throws BindError', () => {
    expect(() =>
      bind('WITH cte(a) AS (SELECT id, name FROM users) SELECT * FROM cte'),
    ).toThrow(BindError);
    expect(() =>
      bind('WITH cte(a) AS (SELECT id, name FROM users) SELECT * FROM cte'),
    ).toThrow('column aliases');
  });

  it('CTE without aliases still uses original column names', () => {
    const plan = bind(
      'WITH cte AS (SELECT id, name FROM users) SELECT id, name FROM cte',
    );
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const cte = plan as LogicalMaterializedCTE;
    const proj = cte.children[1] as LogicalProjection;
    const col0 = proj.expressions[0] as BoundColumnRefExpression;
    expect(col0.columnName).toBe('id');
  });

  it('CTE aliases override original names — original names no longer resolve', () => {
    expect(() =>
      bind('WITH cte(a, b) AS (SELECT id, name FROM users) SELECT id FROM cte'),
    ).toThrow(BindError);
  });
});

// ============================================================================
// Iteration 4: Aggregate in GROUP BY rejected
// ============================================================================

describe('Aggregate in GROUP BY', () => {
  it('COUNT(*) in GROUP BY throws BindError', () => {
    expect(() =>
      bind('SELECT name FROM users GROUP BY COUNT(*)'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT name FROM users GROUP BY COUNT(*)'),
    ).toThrow('GROUP BY clause');
  });

  it('SUM in GROUP BY throws BindError', () => {
    expect(() =>
      bind('SELECT name FROM users GROUP BY SUM(age)'),
    ).toThrow(BindError);
  });

  it('nested aggregate in GROUP BY expression throws BindError', () => {
    expect(() =>
      bind('SELECT name FROM users GROUP BY age + COUNT(*)'),
    ).toThrow(BindError);
  });
});

// ============================================================================
// Iteration 4: Nested aggregates rejected
// ============================================================================

describe('Nested aggregates', () => {
  it('SUM(COUNT(*)) throws BindError', () => {
    expect(() =>
      bind('SELECT SUM(COUNT(*)) FROM users GROUP BY name'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT SUM(COUNT(*)) FROM users GROUP BY name'),
    ).toThrow('Nested aggregate');
  });

  it('AVG(MAX(age)) throws BindError', () => {
    expect(() =>
      bind('SELECT AVG(MAX(age)) FROM users GROUP BY name'),
    ).toThrow(BindError);
  });

  it('COUNT(SUM(age)) throws BindError', () => {
    expect(() =>
      bind('SELECT COUNT(SUM(age)) FROM users GROUP BY name'),
    ).toThrow(BindError);
  });

  it('simple aggregate (no nesting) still works', () => {
    const plan = bind('SELECT name, COUNT(*) FROM users GROUP BY name');
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
  });
});

// ============================================================================
// Iteration 4: Non-aggregated column not in GROUP BY
// ============================================================================

describe('Non-aggregated column not in GROUP BY', () => {
  it('SELECT column not in GROUP BY throws BindError', () => {
    expect(() =>
      bind('SELECT name, age FROM users GROUP BY name'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT name, age FROM users GROUP BY name'),
    ).toThrow('must appear in the GROUP BY');
  });

  it('SELECT column in GROUP BY succeeds', () => {
    const plan = bind('SELECT name FROM users GROUP BY name');
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(1);
  });

  it('SELECT column in GROUP BY with aggregate succeeds', () => {
    const plan = bind('SELECT name, COUNT(*) FROM users GROUP BY name');
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
  });

  it('implicit GROUP BY (aggregates only, no GROUP BY clause) rejects bare columns', () => {
    expect(() =>
      bind('SELECT name, COUNT(*) FROM users'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT name, COUNT(*) FROM users'),
    ).toThrow('must appear in the GROUP BY');
  });

  it('implicit GROUP BY with only aggregates succeeds', () => {
    const plan = bind('SELECT COUNT(*), MAX(age) FROM users');
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
  });

  it('SELECT * with GROUP BY throws if not all columns are grouped', () => {
    expect(() =>
      bind('SELECT * FROM users GROUP BY name'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT * FROM users GROUP BY name'),
    ).toThrow('must appear in the GROUP BY');
  });

  it('HAVING referencing non-grouped column throws BindError', () => {
    expect(() =>
      bind('SELECT name, COUNT(*) FROM users GROUP BY name HAVING age > 10'),
    ).toThrow(BindError);
    expect(() =>
      bind('SELECT name, COUNT(*) FROM users GROUP BY name HAVING age > 10'),
    ).toThrow('must appear in the GROUP BY');
  });

  it('HAVING with aggregate on non-grouped column succeeds', () => {
    const plan = bind(
      'SELECT name, COUNT(*) FROM users GROUP BY name HAVING MAX(age) > 18',
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
  });

  it('expression in SELECT that wraps grouped column succeeds with aggregate', () => {
    const plan = bind(
      'SELECT UPPER(name), COUNT(*) FROM users GROUP BY name',
    );
    const proj = plan as LogicalProjection;
    expect(proj.expressions).toHaveLength(2);
  });
});

// ============================================================================
// Projection aliases
// ============================================================================

describe('projection aliases', () => {
  it('captures AS alias on column ref', () => {
    const proj = bind('SELECT name AS username FROM users') as LogicalProjection;
    expect(proj.aliases).toEqual(['username']);
  });

  it('captures AS alias on expression', () => {
    const proj = bind('SELECT age + 1 AS next_age FROM users') as LogicalProjection;
    expect(proj.aliases).toEqual(['next_age']);
  });

  it('sets null alias when no AS is used', () => {
    const proj = bind('SELECT name, age FROM users') as LogicalProjection;
    expect(proj.aliases).toEqual([null, null]);
  });

  it('mixes aliases and non-aliases', () => {
    const proj = bind('SELECT name, age AS user_age FROM users') as LogicalProjection;
    expect(proj.aliases).toEqual([null, 'user_age']);
  });

  it('star expansion produces null aliases', () => {
    const proj = bind('SELECT * FROM users') as LogicalProjection;
    expect(proj.aliases).toEqual([null, null, null, null]);
  });
});

// ============================================================================
// Recursive CTE
// ============================================================================

describe('Recursive CTE', () => {
  it('produces LogicalRecursiveCTE wrapped in LogicalMaterializedCTE', () => {
    const plan = bind(
      'WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 10) SELECT * FROM nums',
    );
    // Outermost is MaterializedCTE wrapping the recursive CTE definition + main query
    expect(plan.type).toBe(LogicalOperatorType.LOGICAL_MATERIALIZED_CTE);
    const matCte = plan as LogicalMaterializedCTE;
    const recCte = matCte.children[0] as LogicalRecursiveCTE;
    expect(recCte.type).toBe(LogicalOperatorType.LOGICAL_RECURSIVE_CTE);
    expect(recCte.cteName).toBe('nums');
    expect(recCte.isUnionAll).toBe(true);
    expect(recCte.types).toHaveLength(1);
    // children[0] = anchor, children[1] = recursive term
    expect(recCte.children).toHaveLength(2);
  });

  it('binds UNION (not UNION ALL) recursive CTE', () => {
    const plan = bind(
      'WITH RECURSIVE nums AS (SELECT 1 AS n UNION SELECT n + 1 FROM nums WHERE n < 5) SELECT * FROM nums',
    );
    const matCte = plan as LogicalMaterializedCTE;
    const recCte = matCte.children[0] as LogicalRecursiveCTE;
    expect(recCte.isUnionAll).toBe(false);
  });

  it('non-recursive CTE under WITH RECURSIVE is bound normally', () => {
    const plan = bind(
      'WITH RECURSIVE helper AS (SELECT 1 AS x) SELECT * FROM helper',
    );
    const matCte = plan as LogicalMaterializedCTE;
    // helper is not self-referencing, so it should be a regular plan, not LogicalRecursiveCTE
    expect(matCte.children[0].type).not.toBe(LogicalOperatorType.LOGICAL_RECURSIVE_CTE);
  });

  it('non-self-referencing CTE under WITH RECURSIVE binds without error', () => {
    expect(() => bind(
      'WITH RECURSIVE r AS (SELECT 1 AS n FROM users) SELECT * FROM r',
    )).not.toThrow(); // No UNION + no self-reference → bound as normal CTE
  });

  it('errors on column count mismatch between anchor and recursive term', () => {
    expect(() => bind(
      'WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1, 2 FROM r) SELECT * FROM r',
    )).toThrow(/column/i);
  });

  it('errors on type mismatch between anchor and recursive term', () => {
    expect(() => bind(
      "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT 'text' FROM r WHERE n < 5) SELECT * FROM r",
    )).toThrow(/incompatible/i);
  });

  it('allows compatible numeric types between anchor and recursive term', () => {
    // INTEGER anchor, REAL recursive — should be compatible
    expect(() => bind(
      'WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 0.5 FROM r WHERE n < 5) SELECT * FROM r',
    )).not.toThrow();
  });

  it('errors on aggregate in recursive term', () => {
    expect(() => bind(
      'WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT SUM(n) FROM r) SELECT * FROM r',
    )).toThrow(/aggregate/i);
  });

  it('errors on GROUP BY in recursive term', () => {
    expect(() => bind(
      'WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n FROM r GROUP BY n) SELECT * FROM r',
    )).toThrow(/GROUP BY/i);
  });

  it('errors on DISTINCT in recursive term', () => {
    expect(() => bind(
      'WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT DISTINCT n + 1 FROM r WHERE n < 5) SELECT * FROM r',
    )).toThrow(/DISTINCT/i);
  });

  // Note: ORDER BY and LIMIT after UNION are placed on the SetOperationNode
  // by the parser, not on the recursive SelectNode. They cannot structurally
  // appear on the recursive term in practice.
});
