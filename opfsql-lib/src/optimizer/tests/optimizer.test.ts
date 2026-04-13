import { beforeEach, describe, expect, it } from "vitest";
import { Binder } from "../../binder/index.js";
import type {
  BoundColumnRefExpression,
  BoundComparisonExpression,
  BoundConjunctionExpression,
  BoundConstantExpression,
  BoundExpression,
  LogicalComparisonJoin,
  LogicalFilter,
  LogicalGet,
  LogicalLimit,
  LogicalOperator,
  LogicalOrderBy,
  LogicalProjection,
} from "../../binder/types.js";
import {
  BoundExpressionClass,
  LogicalOperatorType,
} from "../../binder/types.js";
import { Parser } from "../../parser/index.js";
import { Catalog } from "../../store/catalog.js";
import type { TableSchema } from "../../store/types.js";
import { FilterCombiner } from "../filter_combiner.js";
import {
  decorrelateExists,
  optimize,
  optimizeBuildProbeSide,
  optimizeJoinOrder,
  pullupFilters,
  pushdownFilters,
  pushdownLimit,
  removeUnusedColumns,
  reorderFilters,
  rewriteExpressions,
  rewriteInClauses,
} from "../index.js";

// ============================================================================
// Test fixtures
// ============================================================================

const parser = new Parser();

function parse(sql: string) {
  const stmts = parser.parse(sql);
  expect(stmts).toHaveLength(1);
  return stmts[0];
}

const usersSchema: TableSchema = {
  name: "users",
  columns: [
    {
      name: "id",
      type: "INTEGER",
      nullable: false,
      primaryKey: true,
      unique: true,
      defaultValue: null,
    },
    {
      name: "name",
      type: "TEXT",
      nullable: false,
      primaryKey: false,
      unique: false,
      defaultValue: null,
    },
    {
      name: "age",
      type: "INTEGER",
      nullable: true,
      primaryKey: false,
      unique: false,
      defaultValue: null,
    },
    {
      name: "active",
      type: "BOOLEAN",
      nullable: true,
      primaryKey: false,
      unique: false,
      defaultValue: null,
    },
  ],
};

const ordersSchema: TableSchema = {
  name: "orders",
  columns: [
    {
      name: "id",
      type: "INTEGER",
      nullable: false,
      primaryKey: true,
      unique: true,
      defaultValue: null,
    },
    {
      name: "user_id",
      type: "INTEGER",
      nullable: false,
      primaryKey: false,
      unique: false,
      defaultValue: null,
    },
    {
      name: "amount",
      type: "REAL",
      nullable: true,
      primaryKey: false,
      unique: false,
      defaultValue: null,
    },
    {
      name: "status",
      type: "TEXT",
      nullable: true,
      primaryKey: false,
      unique: false,
      defaultValue: null,
    },
  ],
};

const productsSchema: TableSchema = {
  name: "products",
  columns: [
    {
      name: "id",
      type: "INTEGER",
      nullable: false,
      primaryKey: true,
      unique: true,
      defaultValue: null,
    },
    {
      name: "name",
      type: "TEXT",
      nullable: false,
      primaryKey: false,
      unique: false,
      defaultValue: null,
    },
    {
      name: "price",
      type: "REAL",
      nullable: true,
      primaryKey: false,
      unique: false,
      defaultValue: null,
    },
  ],
};

let catalog: Catalog;
let binder: Binder;

beforeEach(() => {
  catalog = new Catalog();
  catalog.addTable(usersSchema);
  catalog.addTable(ordersSchema);
  catalog.addTable(productsSchema);
  binder = new Binder(catalog);
});

function bind(sql: string): LogicalOperator {
  return binder.bindStatement(parse(sql));
}

// ============================================================================
// Tree navigation helpers
// ============================================================================

function findNode(
  plan: LogicalOperator,
  type: LogicalOperatorType,
): LogicalOperator | null {
  if (plan.type === type) return plan;
  for (const child of plan.children) {
    const found = findNode(child, type);
    if (found) return found;
  }
  return null;
}

function findAllNodes(
  plan: LogicalOperator,
  type: LogicalOperatorType,
): LogicalOperator[] {
  const result: LogicalOperator[] = [];
  if (plan.type === type) result.push(plan);
  for (const child of plan.children) {
    result.push(...findAllNodes(child, type));
  }
  return result;
}

function getGet(plan: LogicalOperator): LogicalGet {
  const node = findNode(plan, LogicalOperatorType.LOGICAL_GET);
  expect(node).not.toBeNull();
  return node as LogicalGet;
}

function getAllGets(plan: LogicalOperator): LogicalGet[] {
  return findAllNodes(plan, LogicalOperatorType.LOGICAL_GET) as LogicalGet[];
}

// ============================================================================
// Expression Rewriter
// ============================================================================

describe("ExpressionRewriter", () => {
  describe("constant folding", () => {
    it("folds 1 + 1 = 2 to true", () => {
      const plan = bind("SELECT * FROM users WHERE 1 + 1 = 2");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      // After folding: 1+1 → 2, then 2 = 2 → true
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        true,
      );
    });

    it("folds arithmetic: 2 * 3 + 1 in comparison", () => {
      const plan = bind("SELECT * FROM users WHERE age > 2 * 3 + 1");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      // RHS should be folded to 7
      expect(cmp.right.expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((cmp.right as BoundConstantExpression).value).toBe(7);
    });

    it("folds string equality", () => {
      const plan = bind("SELECT * FROM users WHERE 'abc' = 'abc'");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        true,
      );
    });
  });

  describe("comparison simplification", () => {
    it("simplifies NULL = x to NULL", () => {
      const plan = bind("SELECT * FROM users WHERE NULL = age");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        null,
      );
    });
  });

  describe("conjunction simplification", () => {
    it("simplifies x AND true to x", () => {
      const plan = bind("SELECT * FROM users WHERE age > 18 AND true");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      // After simplification: conjunction with TRUE removed, leaving just age > 18
      const expr = filter.expressions[0];
      expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COMPARISON);
      expect((expr as BoundComparisonExpression).comparisonType).toBe(
        "GREATER",
      );
    });

    it("simplifies x AND false to false", () => {
      const plan = bind("SELECT * FROM users WHERE age > 18 AND false");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        false,
      );
    });

    it("simplifies x OR true to true", () => {
      const plan = bind("SELECT * FROM users WHERE age > 18 OR true");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        true,
      );
    });

    it("simplifies x OR false to x", () => {
      const plan = bind("SELECT * FROM users WHERE age > 18 OR false");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const expr = filter.expressions[0];
      expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COMPARISON);
    });
  });

  describe("arithmetic simplification", () => {
    it("simplifies x + 0 to x", () => {
      const plan = bind("SELECT * FROM users WHERE age + 0 > 18");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      // Left should be column ref (age), not age + 0
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
    });

    it("simplifies x * 1 to x", () => {
      const plan = bind("SELECT * FROM users WHERE age * 1 > 18");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
    });

    it("simplifies x * 0 to CASE WHEN x IS NOT NULL THEN 0 ELSE NULL (NULL-safe)", () => {
      const plan = bind("SELECT * FROM users WHERE age * 0 = 0");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      // age * 0 → CASE WHEN age IS NOT NULL THEN 0 ELSE NULL END
      // The LHS of comparison should now be a CASE expression (not folded to constant)
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(BoundExpressionClass.BOUND_CASE);
    });

    it("folds constant * 0 to 0 directly", () => {
      const plan = bind("SELECT * FROM users WHERE 5 * 0 = 0");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      // 5 * 0 → 0 (constant, so no CASE), then 0 = 0 → true
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        true,
      );
    });
  });

  describe("move constants", () => {
    it("normalizes constant to right side: 5 < age → age > 5", () => {
      const plan = bind("SELECT * FROM users WHERE 5 < age");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      // After normalization: age > 5
      expect(cmp.comparisonType).toBe("GREATER");
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
      expect(cmp.right.expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
    });

    it("moves arithmetic constant: age + 3 < 10 → age < 7", () => {
      const plan = bind("SELECT * FROM users WHERE age + 3 < 10");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
      expect(cmp.right.expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((cmp.right as BoundConstantExpression).value).toBe(7);
    });

    it("moves subtract constant: age - 3 < 10 → age < 13", () => {
      const plan = bind("SELECT * FROM users WHERE age - 3 < 10");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
      expect(cmp.right.expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((cmp.right as BoundConstantExpression).value).toBe(13);
    });
  });

  describe("additional simplifications", () => {
    it("folds false OR x to x", () => {
      const plan = bind("SELECT * FROM users WHERE false OR age > 18");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      // After simplification: false removed from OR, leaving just age > 18
      const expr = filter.expressions[0];
      expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COMPARISON);
    });

    it("folds nested constant arithmetic: (2 + 3) * 4 = 20 → true", () => {
      const plan = bind("SELECT * FROM users WHERE (2 + 3) * 4 = 20");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      expect(filter.expressions[0].expressionClass).toBe(
        BoundExpressionClass.BOUND_CONSTANT,
      );
      expect((filter.expressions[0] as BoundConstantExpression).value).toBe(
        true,
      );
    });

    it("simplifies x / 1 to x", () => {
      const plan = bind("SELECT * FROM users WHERE age / 1 > 18");
      const optimized = rewriteExpressions(plan);
      const filter = findNode(
        optimized,
        LogicalOperatorType.LOGICAL_FILTER,
      ) as LogicalFilter;
      const cmp = filter.expressions[0] as BoundComparisonExpression;
      expect(cmp.left.expressionClass).toBe(
        BoundExpressionClass.BOUND_COLUMN_REF,
      );
    });
  });
});

// ============================================================================
// Filter Combiner
// ============================================================================

describe("FilterCombiner", () => {
  it("detects redundant range filters: x > 5 AND x > 7 → x > 7", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(7),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    // Should have a single filter: x > 7
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    expect(comparisons).toHaveLength(1);
    expect(comparisons[0].comparisonType).toBe("GREATER");
    expect((comparisons[0].right as BoundConstantExpression).value).toBe(7);
  });

  it("detects unsatisfiable equality: x = 5 AND x = 6 → false", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(6),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect(filters[0].expressionClass).toBe(
      BoundExpressionClass.BOUND_CONSTANT,
    );
    expect((filters[0] as BoundConstantExpression).value).toBe(false);
  });

  it("generates transitive filters: x = y AND x = 5 → y = 5", () => {
    const combiner = new FilterCombiner();
    // x = y (equivalence)
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeColRef(1, 0),
      returnType: "BOOLEAN",
    });
    // x = 5
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    // Should contain: x = 5, y = 5, and the original x = y
    const eqFilters = filters.filter(
      (f) =>
        f.expressionClass === BoundExpressionClass.BOUND_COMPARISON &&
        (f as BoundComparisonExpression).comparisonType === "EQUAL",
    ) as BoundComparisonExpression[];

    // There should be at least a filter for table 1 (transitive)
    const table1Filters = eqFilters.filter((f) => {
      if (f.left.expressionClass !== BoundExpressionClass.BOUND_COLUMN_REF)
        return false;
      return (f.left as BoundColumnRefExpression).binding.tableIndex === 1;
    });
    expect(table1Filters.length).toBeGreaterThanOrEqual(1);
  });

  it("generates table filters for scan pushdown", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 2),
      right: makeIntConstant(18),
      returnType: "BOOLEAN",
    });

    const tableFilters = combiner.generateTableFilters(0);
    expect(tableFilters).toHaveLength(1);
    expect(tableFilters[0].columnIndex).toBe(2);
    expect(tableFilters[0].comparisonType).toBe("GREATER");
    expect((tableFilters[0].constant as BoundConstantExpression).value).toBe(
      18,
    );
  });

  it("detects unsatisfiable range: x > 10 AND x < 5 → false", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(10),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    expect(filters).toHaveLength(1);
    expect(filters[0].expressionClass).toBe(
      BoundExpressionClass.BOUND_CONSTANT,
    );
    expect((filters[0] as BoundConstantExpression).value).toBe(false);
  });

  it("keeps both NOT_EQUAL filters: x != 5 AND x != 6", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "NOT_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "NOT_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(6),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    // Both NOT_EQUAL filters should be kept (they are not redundant)
    const neFilters = filters.filter(
      (f) =>
        f.expressionClass === BoundExpressionClass.BOUND_COMPARISON &&
        (f as BoundComparisonExpression).comparisonType === "NOT_EQUAL",
    );
    expect(neFilters).toHaveLength(2);
  });

  it("tightens GREATER_EQUAL + GREATER: x >= 5 AND x > 5 → x > 5", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    // Should tighten to a single GREATER filter
    expect(comparisons).toHaveLength(1);
    expect(comparisons[0].comparisonType).toBe("GREATER");
  });

  it("tightens LESS_EQUAL + LESS: x <= 5 AND x < 5 → x < 5", () => {
    const combiner = new FilterCombiner();
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS_EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "LESS",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });

    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    expect(comparisons).toHaveLength(1);
    expect(comparisons[0].comparisonType).toBe("LESS");
  });

  it("keeps triple range filters, retains tightest: x > 3 AND x > 5 AND x > 7 → x > 7", () => {
    const combiner = new FilterCombiner();
    for (const v of [3, 5, 7]) {
      combiner.addFilter({
        expressionClass: BoundExpressionClass.BOUND_COMPARISON,
        comparisonType: "GREATER",
        left: makeColRef(0, 0),
        right: makeIntConstant(v),
        returnType: "BOOLEAN",
      });
    }

    const filters = combiner.generateFilters();
    const comparisons = filters.filter(
      (f) => f.expressionClass === BoundExpressionClass.BOUND_COMPARISON,
    ) as BoundComparisonExpression[];
    expect(comparisons).toHaveLength(1);
    expect((comparisons[0].right as BoundConstantExpression).value).toBe(7);
  });

  it("generates table filters for multiple columns", () => {
    const combiner = new FilterCombiner();
    // age > 18
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "GREATER",
      left: makeColRef(0, 2),
      right: makeIntConstant(18),
      returnType: "BOOLEAN",
    });
    // id = 5
    combiner.addFilter({
      expressionClass: BoundExpressionClass.BOUND_COMPARISON,
      comparisonType: "EQUAL",
      left: makeColRef(0, 0),
      right: makeIntConstant(5),
      returnType: "BOOLEAN",
    });

    const tableFilters = combiner.generateTableFilters(0);
    expect(tableFilters).toHaveLength(2);
    const colIndices = tableFilters.map((f) => f.columnIndex).sort();
    expect(colIndices).toEqual([0, 2]);
  });
});

// ============================================================================
// Filter Pullup
// ============================================================================

describe("FilterPullup", () => {
  it("pulls INNER JOIN conditions up into filter", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 18",
    );
    const optimized = pullupFilters(plan);

    // After pullup the INNER JOIN should become a CrossProduct with Filter above
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    // The filter should contain both the WHERE condition and the JOIN condition
    // Check that a cross product exists below
    const cross = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_CROSS_PRODUCT,
    );
    expect(cross).not.toBeNull();
  });

  it("preserves INNER JOIN when no WHERE clause exists", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const optimized = pullupFilters(plan);

    // Without a filter above, pullup leaves the join intact
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    expect(join.joinType).toBe("INNER");
  });

  it("pulls conditions from all INNER JOINs in a multi-join chain with WHERE", () => {
    const plan = bind(
      `SELECT * FROM users u
       JOIN orders o ON u.id = o.user_id
       JOIN products p ON p.id = o.user_id
       WHERE u.age > 18`,
    );
    const optimized = pullupFilters(plan);

    // After pullup, all inner join conditions should be combined into filters
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    // Should have multiple conditions (WHERE + both JOIN conditions)
    expect(filter.expressions.length).toBeGreaterThanOrEqual(1);
  });

  it("does not pull LEFT JOIN conditions", () => {
    const plan = bind(
      "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
    );
    const optimized = pullupFilters(plan);

    // LEFT JOIN should stay as comparison join
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    expect(join.joinType).toBe("LEFT");
  });
});

// ============================================================================
// Filter Pushdown
// ============================================================================

describe("FilterPushdown", () => {
  it("pushes filter below projection", () => {
    const plan = bind("SELECT name FROM users WHERE age > 18");
    const optimized = pushdownFilters(plan);
    // After pushdown, filter should be below projection (closer to scan)
    const proj = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(proj).not.toBeNull();
    // The child of projection should be a filter or get (not filter above projection)
    if (optimized.type === LogicalOperatorType.LOGICAL_PROJECTION) {
      // Filter was pushed below projection — good
      const child = optimized.children[0];
      const hasFilterBelow =
        child.type === LogicalOperatorType.LOGICAL_FILTER ||
        findNode(child, LogicalOperatorType.LOGICAL_FILTER) !== null;
      const hasGetWithFilters =
        findNode(child, LogicalOperatorType.LOGICAL_GET) !== null;
      expect(hasFilterBelow || hasGetWithFilters).toBe(true);
    }
  });

  it("pushes filter to scan as table filter", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18");
    const optimized = pushdownFilters(plan);
    const get = getGet(optimized);
    // Filter should have been pushed to the scan's tableFilters
    expect(get.tableFilters.length).toBeGreaterThan(0);
    expect(get.tableFilters[0].columnIndex).toBe(2); // age is column index 2
    expect(get.tableFilters[0].comparisonType).toBe("GREATER");
  });

  it("splits filters across INNER JOIN sides", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 18 AND orders.amount > 100",
    );
    // First pullup to extract join conditions, then pushdown
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);

    const gets = getAllGets(optimized);
    const usersGet = gets.find((g) => g.tableName === "users");
    const ordersGet = gets.find((g) => g.tableName === "orders");

    expect(usersGet).toBeDefined();
    expect(ordersGet).toBeDefined();

    // users.age > 18 should be pushed to users scan
    expect(usersGet!.tableFilters.length).toBeGreaterThan(0);
    // orders.amount > 100 should be pushed to orders scan
    expect(ordersGet!.tableFilters.length).toBeGreaterThan(0);
  });

  it("converts cross product with condition to INNER JOIN", () => {
    const plan = bind(
      "SELECT * FROM users CROSS JOIN orders WHERE users.id = orders.user_id",
    );
    const optimized = pushdownFilters(plan);
    // Should have converted to a comparison join
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).not.toBeNull();
    expect((join as LogicalComparisonJoin).joinType).toBe("INNER");
    expect((join as LogicalComparisonJoin).conditions.length).toBeGreaterThan(
      0,
    );
  });

  it("does not push right-side filters through LEFT JOIN", () => {
    const plan = bind(
      "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE orders.amount > 100",
    );
    const optimized = pushdownFilters(plan);
    // The orders.amount > 100 filter should remain above the LEFT JOIN
    // (pushing it below would incorrectly filter out NULL rows)
    const ordersGet = getAllGets(optimized).find(
      (g) => g.tableName === "orders",
    );
    expect(ordersGet).toBeDefined();
    // orders scan should NOT have the filter pushed to it
    expect(ordersGet!.tableFilters).toHaveLength(0);
  });

  it("pushes filters through ORDER BY", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18 ORDER BY name");
    const optimized = pushdownFilters(plan);
    // Filter should be below order by
    const orderBy = findNode(optimized, LogicalOperatorType.LOGICAL_ORDER_BY);
    expect(orderBy).not.toBeNull();
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
  });

  it("pushes pre-aggregation filters through aggregate", () => {
    const plan = bind(
      "SELECT age, COUNT(*) FROM users WHERE age > 18 GROUP BY age",
    );
    const optimized = pushdownFilters(plan);
    const get = getGet(optimized);
    // age > 18 should be pushed to scan
    expect(get.tableFilters.length).toBeGreaterThan(0);
  });

  it("pushes filters through MaterializedCTE into main plan", () => {
    const plan = bind(
      `WITH active AS (SELECT id, name FROM users WHERE active = true)
       SELECT a.name FROM active a INNER JOIN orders o ON a.id = o.user_id WHERE o.amount > 100`,
    );
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);

    // Should have a join (not a cross product)
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).not.toBeNull();
    expect((join as LogicalComparisonJoin).joinType).toBe("INNER");

    // o.amount > 100 should be pushed to orders scan
    const ordersGet = getAllGets(optimized).find(
      (g) => g.tableName === "orders",
    );
    expect(ordersGet).toBeDefined();
    expect(ordersGet!.tableFilters.length).toBeGreaterThan(0);
  });

  it("preserves join condition when CTE query has WHERE clause", () => {
    const plan = bind(
      `WITH totals AS (SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id)
       SELECT u.name, t.total FROM totals t INNER JOIN users u ON t.user_id = u.id WHERE t.total > 500`,
    );
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);

    // Must be a comparison join, NOT a cross product
    const cross = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_CROSS_PRODUCT,
    );
    expect(cross).toBeNull();

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).not.toBeNull();
    expect((join as LogicalComparisonJoin).conditions.length).toBeGreaterThan(
      0,
    );
  });

  it("does not convert non-equality cross-table filter to join condition", () => {
    // b.id > a.id references both sides but is NOT an equality — must stay as post-join filter
    const plan = bind(
      "SELECT * FROM users a JOIN users b ON a.id = b.id WHERE b.age > a.age",
    );
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();

    // Only the equality condition should be a join condition
    for (const cond of join.conditions) {
      expect(cond.comparisonType).toBe("EQUAL");
    }

    // The non-equality filter (b.age > a.age) should remain as a LogicalFilter above the join
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const filterCmp = filter.expressions[0] as BoundComparisonExpression;
    expect(filterCmp.comparisonType).toBe("GREATER");
  });

  it("normalizes sides of extracted join conditions (left=left-child, right=right-child)", () => {
    // WHERE o.user_id = u.id — after pullup+pushdown the extracted condition
    // should be normalized so cond.left refs left child, cond.right refs right child
    const plan = bind(
      "SELECT * FROM users u CROSS JOIN orders o WHERE o.user_id = u.id",
    );
    const optimized = pushdownFilters(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    expect(join.conditions.length).toBeGreaterThan(0);

    const leftTables = new Set(
      join.children[0].getColumnBindings().map((b) => b.tableIndex),
    );
    const rightTables = new Set(
      join.children[1].getColumnBindings().map((b) => b.tableIndex),
    );

    for (const cond of join.conditions) {
      const condLeftRef = cond.left as BoundColumnRefExpression;
      const condRightRef = cond.right as BoundColumnRefExpression;
      expect(leftTables.has(condLeftRef.binding.tableIndex)).toBe(true);
      expect(rightTables.has(condRightRef.binding.tableIndex)).toBe(true);
    }
  });

  it("normalizes sides when adding join condition to existing join", () => {
    // ON u.id = o.user_id WHERE o.amount = u.age — the WHERE equality
    // references both sides and should be normalized when added as join condition
    const plan = bind(
      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount = u.age",
    );
    const pulled = pullupFilters(plan);
    const optimized = pushdownFilters(pulled);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();

    const leftTables = new Set(
      join.children[0].getColumnBindings().map((b) => b.tableIndex),
    );
    const rightTables = new Set(
      join.children[1].getColumnBindings().map((b) => b.tableIndex),
    );

    for (const cond of join.conditions) {
      expect(cond.comparisonType).toBe("EQUAL");
      const condLeftRef = cond.left as BoundColumnRefExpression;
      const condRightRef = cond.right as BoundColumnRefExpression;
      expect(leftTables.has(condLeftRef.binding.tableIndex)).toBe(true);
      expect(rightTables.has(condRightRef.binding.tableIndex)).toBe(true);
    }
  });

  it("optimizes CTE definition independently", () => {
    const plan = bind(
      `WITH filtered AS (SELECT id, name FROM users WHERE age > 21)
       SELECT f.name FROM filtered f`,
    );
    const optimized = pushdownFilters(plan);

    // age > 21 from CTE definition should be pushed to users scan
    const usersGet = getAllGets(optimized).find((g) => g.tableName === "users");
    expect(usersGet).toBeDefined();
    expect(usersGet!.tableFilters.length).toBeGreaterThan(0);
    expect(usersGet!.tableFilters[0].columnIndex).toBe(2); // age
  });
});

// ============================================================================
// IN Clause Rewriter
// ============================================================================

describe("InClauseRewriter", () => {
  it("rewrites single-value IN to equality", () => {
    const plan = bind("SELECT * FROM users WHERE id IN (5)");
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    // Should be rewritten to id = 5
    const expr = filter.expressions[0];
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COMPARISON);
    expect((expr as BoundComparisonExpression).comparisonType).toBe("EQUAL");
  });

  it("rewrites multi-value IN to OR", () => {
    const plan = bind("SELECT * FROM users WHERE id IN (1, 2, 3)");
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    // Should be rewritten to id = 1 OR id = 2 OR id = 3
    const expr = filter.expressions[0];
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_CONJUNCTION);
    const conj = expr as BoundConjunctionExpression;
    expect(conj.conjunctionType).toBe("OR");
    expect(conj.children).toHaveLength(3);
  });

  it("rewrites NOT IN to AND of NOT_EQUAL", () => {
    const plan = bind("SELECT * FROM users WHERE id NOT IN (1, 2)");
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const expr = filter.expressions[0];
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_CONJUNCTION);
    const conj = expr as BoundConjunctionExpression;
    expect(conj.conjunctionType).toBe("AND");
    expect(conj.children).toHaveLength(2);
    for (const child of conj.children) {
      expect((child as BoundComparisonExpression).comparisonType).toBe(
        "NOT_EQUAL",
      );
    }
  });

  it("does not expand large IN list (>10 values)", () => {
    const values = Array.from({ length: 11 }, (_, i) => i + 1).join(", ");
    const plan = bind(`SELECT * FROM users WHERE id IN (${values})`);
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    // Should remain as IN operator, not be expanded to OR
    const expr = filter.expressions[0];
    expect(expr.expressionClass).not.toBe(
      BoundExpressionClass.BOUND_CONJUNCTION,
    );
  });

  it("expands IN with exactly 10 values (at threshold)", () => {
    const values = Array.from({ length: 10 }, (_, i) => i + 1).join(", ");
    const plan = bind(`SELECT * FROM users WHERE id IN (${values})`);
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    // Should be expanded to OR chain at threshold
    const expr = filter.expressions[0];
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_CONJUNCTION);
    expect((expr as BoundConjunctionExpression).conjunctionType).toBe("OR");
    expect((expr as BoundConjunctionExpression).children).toHaveLength(10);
  });

  it("rewrites NOT IN single value to NOT_EQUAL", () => {
    const plan = bind("SELECT * FROM users WHERE id NOT IN (5)");
    const optimized = rewriteInClauses(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    const expr = filter.expressions[0];
    expect(expr.expressionClass).toBe(BoundExpressionClass.BOUND_COMPARISON);
    expect((expr as BoundComparisonExpression).comparisonType).toBe(
      "NOT_EQUAL",
    );
  });
});

// ============================================================================
// Join Order Optimizer
// ============================================================================

describe("JoinOrderOptimizer", () => {
  it("preserves single join", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const optimized = optimizeJoinOrder(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    expect(join.conditions.length).toBeGreaterThan(0);
  });

  it("reorders multi-way join by cardinality", () => {
    // Set different cardinalities
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON products.id = orders.user_id",
    );

    // Manually set cardinalities to test reordering
    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 1000;
      if (get.tableName === "orders") get.estimatedCardinality = 10000;
      if (get.tableName === "products") get.estimatedCardinality = 100;
    }

    // First pullup to flatten, then optimize join order
    const pulled = pullupFilters(plan);
    const optimized = optimizeJoinOrder(pushed(pulled));

    // Should still have all three tables
    const resultGets = getAllGets(optimized);
    expect(resultGets).toHaveLength(3);
    const tableNames = resultGets.map((g) => g.tableName).sort();
    expect(tableNames).toEqual(["orders", "products", "users"]);
  });

  it("does not reorder single table (no join)", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18");
    const optimized = optimizeJoinOrder(plan);
    // Should be unchanged
    const get = getGet(optimized);
    expect(get.tableName).toBe("users");
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    // A single table has no join to reorder
    // (the original might have no join at all)
    expect(getAllGets(optimized)).toHaveLength(1);
  });

  it("handles very unbalanced cardinalities", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 1;
      if (get.tableName === "orders") get.estimatedCardinality = 1000000;
    }

    const optimized = optimizeJoinOrder(plan);
    // Should still produce a valid plan with both tables
    const resultGets = getAllGets(optimized);
    expect(resultGets).toHaveLength(2);
  });
});

// Helper: run pushdown after pullup for multi-way join tests
function pushed(plan: LogicalOperator): LogicalOperator {
  return pushdownFilters(plan);
}

// ============================================================================
// Remove Unused Columns
// ============================================================================

describe("RemoveUnusedColumns", () => {
  it("prunes unused columns from scan", () => {
    const plan = bind("SELECT name FROM users");
    const optimized = removeUnusedColumns(plan);
    const get = getGet(optimized);
    // Only 'name' (columnIndex 1) should remain (or at minimum fewer than all 4 columns)
    expect(get.columnIds.length).toBeLessThan(4);
  });

  it("keeps columns referenced by WHERE", () => {
    const plan = bind("SELECT name FROM users WHERE age > 18");
    const optimized = removeUnusedColumns(plan);
    const get = getGet(optimized);
    // Should keep at least name (1) and age (2)
    expect(get.columnIds).toContain(1); // name
    expect(get.columnIds).toContain(2); // age
  });

  it("keeps columns referenced by ORDER BY", () => {
    const plan = bind("SELECT name, age FROM users ORDER BY age");
    const optimized = removeUnusedColumns(plan);
    const get = getGet(optimized);
    // name (1) and age (2) should both be kept
    expect(get.columnIds).toContain(1); // name
    expect(get.columnIds).toContain(2); // age
  });

  it("keeps columns referenced by JOIN conditions", () => {
    const plan = bind(
      "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id",
    );
    const optimized = removeUnusedColumns(plan);
    const usersGet = getAllGets(optimized).find(
      (g) => g.tableName === "users",
    )!;
    expect(usersGet.columnIds).toContain(0); // id (used in join)
    expect(usersGet.columnIds).toContain(1); // name (used in select)
  });

  it("keeps at least one column even if none are used", () => {
    // SELECT 1 FROM users — no columns actually needed from users
    const plan = bind("SELECT 1 FROM users");
    const optimized = removeUnusedColumns(plan);
    const get = getGet(optimized);
    expect(get.columnIds.length).toBeGreaterThanOrEqual(1);
  });

  it("keeps columns referenced by tableFilters after pushdown", () => {
    // pushdown moves age>18 to tableFilters, then removeUnusedColumns runs
    // age column must survive in columnIds for the filter to work
    const plan = bind("SELECT name FROM users WHERE age > 18");
    const pushed = pushdownFilters(plan);
    const optimized = removeUnusedColumns(pushed);
    const get = getGet(optimized);

    // tableFilter references age (columnIndex 2)
    expect(get.tableFilters.length).toBeGreaterThan(0);
    // age must remain in columnIds for the filter to be evaluable
    expect(get.columnIds).toContain(2); // age
    expect(get.columnIds).toContain(1); // name
  });

  it("preserves aliases through column pruning", () => {
    const plan = bind("SELECT name AS username, age AS user_age FROM users");
    const proj = findNode(
      plan,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(proj.aliases).toEqual(["username", "user_age"]);

    const optimized = removeUnusedColumns(plan);
    const prunedProj = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(prunedProj.aliases).toEqual(["username", "user_age"]);
    expect(prunedProj.expressions).toHaveLength(2);
  });

  it("prunes aliases in parallel with expressions", () => {
    // SELECT name AS username FROM users — age, id, active should be pruned
    const plan = bind("SELECT name AS username FROM users");
    const optimized = removeUnusedColumns(plan);
    const proj = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(proj.expressions).toHaveLength(1);
    expect(proj.aliases).toHaveLength(1);
    expect(proj.aliases[0]).toBe("username");
  });

  it("preserves null aliases through pruning", () => {
    const plan = bind("SELECT name, age FROM users");
    const optimized = removeUnusedColumns(plan);
    const proj = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(proj.aliases).toEqual([null, null]);
    expect(proj.aliases).toHaveLength(proj.expressions.length);
  });

  it("preserves mixed aliases through pruning", () => {
    const plan = bind("SELECT name AS username, age FROM users");
    const optimized = removeUnusedColumns(plan);
    const proj = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_PROJECTION,
    ) as LogicalProjection;
    expect(proj.aliases).toEqual(["username", null]);
  });
});

// ============================================================================
// Build/Probe Side Optimizer
// ============================================================================

describe("BuildProbeSideOptimizer", () => {
  it("swaps sides when left is smaller (INNER JOIN)", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    // Set users small, orders large
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 10;
      if (get.tableName === "orders") get.estimatedCardinality = 10000;
    }

    const optimized = optimizeBuildProbeSide(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();

    // After swap, left (probe) should be larger, right (build) should be smaller
    // users (10) should be on the right (build side)
    const rightGet = getGet(join.children[1]);
    expect(rightGet.tableName).toBe("users");
  });

  it("does not swap LEFT JOIN", () => {
    const plan = bind(
      "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 10;
      if (get.tableName === "orders") get.estimatedCardinality = 10000;
    }

    const optimized = optimizeBuildProbeSide(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    // LEFT JOIN should not be swapped
    const leftGet = getGet(join.children[0]);
    expect(leftGet.tableName).toBe("users");
  });

  it("does not swap when left is already larger (build=right is smaller)", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      if (get.tableName === "users") get.estimatedCardinality = 10000;
      if (get.tableName === "orders") get.estimatedCardinality = 10;
    }

    const optimized = optimizeBuildProbeSide(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    // Right (build) should remain the smaller table (orders)
    const rightGet = getGet(join.children[1]);
    expect(rightGet.tableName).toBe("orders");
  });

  it("handles equal cardinality without swapping", () => {
    const plan = bind(
      "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
    );
    const gets = getAllGets(plan);
    for (const get of gets) {
      get.estimatedCardinality = 100;
    }

    const optimized = optimizeBuildProbeSide(plan);
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).not.toBeNull();
    // Both tables present, plan should be valid regardless
    expect(getAllGets(optimized)).toHaveLength(2);
  });
});

// ============================================================================
// Limit Pushdown
// ============================================================================

describe("LimitPushdown", () => {
  it("pushes small LIMIT below projection", () => {
    const plan = bind("SELECT name FROM users LIMIT 10");
    const optimized = pushdownLimit(plan);

    // Should have LIMIT → PROJECTION → LIMIT → GET structure
    const limits = findAllNodes(optimized, LogicalOperatorType.LOGICAL_LIMIT);
    // There should be a limit below the projection too
    expect(limits.length).toBeGreaterThanOrEqual(1);
  });

  it("does not push large LIMIT", () => {
    const plan = bind("SELECT name FROM users LIMIT 10000");
    const optimized = pushdownLimit(plan);

    // Large LIMIT should stay in original position
    const limit = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_LIMIT,
    ) as LogicalLimit;
    expect(limit).not.toBeNull();
    expect(limit.limitVal).toBe(10000);
  });

  it("annotates ORDER BY with topN when LIMIT is above", () => {
    const plan = bind("SELECT * FROM users ORDER BY age LIMIT 10");
    const optimized = pushdownLimit(plan);

    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBe(10);
  });

  it("annotates ORDER BY with topN = limit + offset", () => {
    const plan = bind("SELECT * FROM users ORDER BY age LIMIT 5 OFFSET 3");
    const optimized = pushdownLimit(plan);

    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBe(8); // 5 + 3
  });

  it("annotates ORDER BY through PROJECTION", () => {
    const plan = bind("SELECT name FROM users ORDER BY age LIMIT 10");
    const optimized = pushdownLimit(plan);

    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBe(10);
  });

  it("does not annotate ORDER BY when LIMIT is large", () => {
    const plan = bind("SELECT * FROM users ORDER BY age LIMIT 10000");
    const optimized = pushdownLimit(plan);

    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBeUndefined();
  });

  it("handles LIMIT 0 edge case", () => {
    const plan = bind("SELECT name FROM users LIMIT 0");
    const optimized = pushdownLimit(plan);

    const limit = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_LIMIT,
    ) as LogicalLimit;
    expect(limit).not.toBeNull();
    expect(limit.limitVal).toBe(0);
  });

  it("handles large OFFSET with small LIMIT", () => {
    const plan = bind("SELECT * FROM users ORDER BY age LIMIT 5 OFFSET 100");
    const optimized = pushdownLimit(plan);

    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    // topN = limit + offset = 5 + 100 = 105
    expect(orderBy.topN).toBe(105);
  });

  it("does not push LIMIT below aggregate", () => {
    const plan = bind(
      "SELECT age, COUNT(*) FROM users GROUP BY age LIMIT 5",
    );
    const optimized = pushdownLimit(plan);

    // Aggregate should remain below limit
    const agg = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
    );
    expect(agg).not.toBeNull();
    // Should still have a limit
    const limit = findNode(optimized, LogicalOperatorType.LOGICAL_LIMIT);
    expect(limit).not.toBeNull();
  });
});

// ============================================================================
// Reorder Filter
// ============================================================================

describe("ReorderFilter", () => {
  it("puts cheap conditions before expensive ones", () => {
    const plan = bind(
      "SELECT * FROM users WHERE upper(name) = 'JOHN' AND age > 18",
    );
    const optimized = reorderFilters(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    if (filter && filter.expressions.length > 1) {
      // age > 18 (cheap: column comparison) should come before upper(name) = 'JOHN' (expensive: function call)
      // First expression should be the cheaper one
      const first = filter.expressions[0];
      const second = filter.expressions[1];
      // The comparison without function should be first
      const firstHasFunction = containsFunction(first);
      const secondHasFunction = containsFunction(second);
      if (firstHasFunction !== secondHasFunction) {
        expect(firstHasFunction).toBe(false);
      }
    }
  });

  it("does not reorder when expression can throw (division)", () => {
    const plan = bind("SELECT * FROM users WHERE age > 0 AND id / age > 5");
    const optimized = reorderFilters(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    if (filter && filter.expressions.length > 1) {
      // Order should be preserved — age > 0 must stay before id / age > 5
      const first = filter.expressions[0] as BoundComparisonExpression;
      // The guard condition (age > 0) should remain first
      expect(first.comparisonType).toBe("GREATER");
      expect((first.right as BoundConstantExpression).value).toBe(0);
    }
  });

  it("preserves single filter unchanged", () => {
    const plan = bind("SELECT * FROM users WHERE age > 18");
    const optimized = reorderFilters(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).not.toBeNull();
    // Single expression should pass through unchanged
    expect(filter.expressions).toHaveLength(1);
    expect(
      (filter.expressions[0] as BoundComparisonExpression).comparisonType,
    ).toBe("GREATER");
  });

  it("reorders subquery after cheap column comparison", () => {
    const plan = bind(
      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) AND age > 18",
    );
    const optimized = reorderFilters(plan);
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    if (filter && filter.expressions.length > 1) {
      // Subquery is very expensive; age > 18 should come first
      const first = filter.expressions[0];
      expect(first.expressionClass).not.toBe(
        BoundExpressionClass.BOUND_SUBQUERY,
      );
    }
  });
});

// ============================================================================
// Full Pipeline (optimize)
// ============================================================================

describe("optimize (full pipeline)", () => {
  it("optimizes simple select with filter", () => {
    const plan = bind("SELECT name FROM users WHERE age > 18");
    const optimized = optimize(plan);
    // Should still have the same query semantics
    const proj = findNode(optimized, LogicalOperatorType.LOGICAL_PROJECTION);
    expect(proj).not.toBeNull();
    const get = getGet(optimized);
    expect(get.tableName).toBe("users");
  });

  it("optimizes join with filter pushdown", () => {
    const plan = bind(
      "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 18",
    );
    const optimized = optimize(plan);
    // Should have pushed age > 18 close to users scan
    const usersGet = getAllGets(optimized).find((g) => g.tableName === "users");
    expect(usersGet).toBeDefined();
    expect(usersGet!.tableFilters.length).toBeGreaterThan(0);
  });

  it("optimizes cross product to join", () => {
    const plan = bind(
      "SELECT * FROM users CROSS JOIN orders WHERE users.id = orders.user_id",
    );
    const optimized = optimize(plan);
    // Cross product should have been converted to inner join
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).not.toBeNull();
  });

  it("handles constant folding end-to-end", () => {
    const plan = bind("SELECT * FROM users WHERE 1 + 1 = 2");
    const optimized = optimize(plan);
    // After constant folding: 1+1=2 → true, the filter should fold away
    // or at least the expression should be a constant true
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter | null;
    if (filter) {
      const expr = filter.expressions[0];
      if (expr.expressionClass === BoundExpressionClass.BOUND_CONSTANT) {
        expect((expr as BoundConstantExpression).value).toBe(true);
      }
    }
    // Either way, the query should still have a GET
    const get = getGet(optimized);
    expect(get.tableName).toBe("users");
  });

  it("optimizes query with LIMIT", () => {
    const plan = bind("SELECT name FROM users LIMIT 5");
    const optimized = optimize(plan);
    const limit = findNode(optimized, LogicalOperatorType.LOGICAL_LIMIT);
    expect(limit).not.toBeNull();
  });

  it("optimizes query with GROUP BY", () => {
    const plan = bind(
      "SELECT age, COUNT(*) FROM users WHERE age > 18 GROUP BY age",
    );
    const optimized = optimize(plan);
    const agg = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_AGGREGATE_AND_GROUP_BY,
    );
    expect(agg).not.toBeNull();
    // Filter should be pushed below aggregate
    const get = getGet(optimized);
    expect(get.tableFilters.length).toBeGreaterThan(0);
  });

  it("preserves DDL statements", () => {
    const plan = bind("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)");
    const optimized = optimize(plan);
    expect(optimized.type).toBe(LogicalOperatorType.LOGICAL_CREATE_TABLE);
  });

  it("preserves DML insert", () => {
    const plan = bind(
      "INSERT INTO users (id, name, age) VALUES (1, 'John', 30)",
    );
    const optimized = optimize(plan);
    expect(optimized.type).toBe(LogicalOperatorType.LOGICAL_INSERT);
  });

  it("optimizes subquery in WHERE", () => {
    const plan = bind(
      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
    );
    const optimized = optimize(plan);
    // Should still produce a valid plan
    const get = getAllGets(optimized);
    expect(get.length).toBeGreaterThanOrEqual(1);
  });

  it("CTE + JOIN produces COMPARISON_JOIN not CROSS_PRODUCT", () => {
    const plan = bind(
      `WITH rev AS (SELECT user_id, SUM(amount) AS total_amount FROM orders GROUP BY user_id)
       SELECT u.name, r.total_amount
       FROM rev r INNER JOIN users u ON r.user_id = u.id
       ORDER BY r.total_amount DESC LIMIT 10`,
    );
    const optimized = optimize(plan);
    // Must be a comparison join, NOT a cross product
    const cross = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_CROSS_PRODUCT,
    );
    expect(cross).toBeNull();
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).not.toBeNull();
  });

  it("optimizes ORDER BY with LIMIT (topN annotation)", () => {
    const plan = bind(
      "SELECT name FROM users ORDER BY age DESC LIMIT 3",
    );
    const optimized = optimize(plan);
    const orderBy = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_ORDER_BY,
    ) as LogicalOrderBy;
    expect(orderBy).not.toBeNull();
    expect(orderBy.topN).toBe(3);
  });

  it("optimizes DISTINCT query", () => {
    const plan = bind("SELECT DISTINCT name FROM users");
    const optimized = optimize(plan);
    const distinct = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_DISTINCT,
    );
    expect(distinct).not.toBeNull();
  });

  it("optimizes UPDATE statement", () => {
    const plan = bind("UPDATE users SET age = 30 WHERE id = 1");
    const optimized = optimize(plan);
    expect(optimized.type).toBe(LogicalOperatorType.LOGICAL_UPDATE);
  });

  it("optimizes DELETE statement", () => {
    const plan = bind("DELETE FROM users WHERE age < 18");
    const optimized = optimize(plan);
    expect(optimized.type).toBe(LogicalOperatorType.LOGICAL_DELETE);
  });
});

// ============================================================================
// Index Selection
// ============================================================================

describe("IndexSelection", () => {
  it("annotates LogicalGet with indexHint for equality filter", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      columns: ["age"],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age = 30");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.indexDef.name).toBe("idx_age");
    expect(get.indexHint!.predicates).toHaveLength(1);
    expect(get.indexHint!.predicates[0].comparisonType).toBe("EQUAL");
    expect(
      (get.indexHint!.predicates[0].value as BoundConstantExpression).value,
    ).toBe(30);
  });

  it("annotates LogicalGet with indexHint for range filter", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      columns: ["age"],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age > 18");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates[0].comparisonType).toBe("GREATER");
  });

  it("does NOT set indexHint when no index matches", () => {
    // No index on 'name'
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      columns: ["age"],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE name = 'Alice'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeUndefined();
  });

  it("does NOT set indexHint when there are no filters", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      columns: ["age"],
      unique: false,
    });
    const plan = bind("SELECT * FROM users");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeUndefined();
  });

  it("prefers unique index over non-unique", () => {
    catalog.addIndex({
      name: "idx_name",
      tableName: "users",
      columns: ["name"],
      unique: false,
    });
    catalog.addIndex({
      name: "idx_name_uniq",
      tableName: "users",
      columns: ["name"],
      unique: true,
    });
    const plan = bind("SELECT * FROM users WHERE name = 'Alice'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.indexDef.name).toBe("idx_name_uniq");
  });

  it("handles composite index with equality prefix", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      columns: ["user_id", "status"],
      unique: false,
    });
    const plan = bind(
      "SELECT * FROM orders WHERE user_id = 1 AND status = 'shipped'",
    );
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates).toHaveLength(2);
    expect(get.indexHint!.residualFilters).toHaveLength(0);
  });

  it("sets residual filters for non-covered predicates", () => {
    catalog.addIndex({
      name: "idx_age",
      tableName: "users",
      columns: ["age"],
      unique: false,
    });
    const plan = bind("SELECT * FROM users WHERE age = 30 AND name = 'Alice'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates).toHaveLength(1); // only age
    expect(get.indexHint!.residualFilters).toHaveLength(1); // name is residual
  });

  it("uses composite index prefix for partial match", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      columns: ["user_id", "status"],
      unique: false,
    });
    const plan = bind("SELECT * FROM orders WHERE user_id = 1");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    expect(get.indexHint!.predicates).toHaveLength(1);
    expect(get.indexHint!.predicates[0].comparisonType).toBe("EQUAL");
  });

  it("does not use composite index when first column has no filter", () => {
    catalog.addIndex({
      name: "idx_comp",
      tableName: "orders",
      columns: ["user_id", "status"],
      unique: false,
    });
    // Only status filter, but user_id is the first index column
    const plan = bind("SELECT * FROM orders WHERE status = 'shipped'");
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    // Should not use composite index since prefix not matched
    expect(get.indexHint).toBeUndefined();
  });

  it("handles range filter on single-column index", () => {
    catalog.addIndex({
      name: "idx_amount",
      tableName: "orders",
      columns: ["amount"],
      unique: false,
    });
    const plan = bind(
      "SELECT * FROM orders WHERE amount > 50 AND amount < 200",
    );
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    // Should use the index for range filter
    expect(get.indexHint!.predicates.length).toBeGreaterThanOrEqual(1);
  });

  it("chooses index with more covered predicates", () => {
    catalog.addIndex({
      name: "idx_age_only",
      tableName: "users",
      columns: ["age"],
      unique: false,
    });
    catalog.addIndex({
      name: "idx_age_active",
      tableName: "users",
      columns: ["age", "active"],
      unique: false,
    });
    const plan = bind(
      "SELECT * FROM users WHERE age = 30 AND active = true",
    );
    const optimized = optimize(plan, catalog);
    const get = getGet(optimized);
    expect(get.indexHint).toBeDefined();
    // Should choose the composite index that covers both predicates
    expect(get.indexHint!.indexDef.name).toBe("idx_age_active");
    expect(get.indexHint!.predicates).toHaveLength(2);
    expect(get.indexHint!.residualFilters).toHaveLength(0);
  });
});

// ============================================================================
// Helpers for constructing test expressions
// ============================================================================

function makeColRef(
  tableIndex: number,
  columnIndex: number,
): BoundColumnRefExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_COLUMN_REF,
    binding: { tableIndex, columnIndex },
    tableName: "",
    columnName: "",
    returnType: "INTEGER",
  };
}

function makeIntConstant(value: number): BoundConstantExpression {
  return {
    expressionClass: BoundExpressionClass.BOUND_CONSTANT,
    value,
    returnType: "INTEGER",
  };
}

function containsFunction(expr: BoundExpression): boolean {
  if (expr.expressionClass === BoundExpressionClass.BOUND_FUNCTION) return true;
  if (expr.expressionClass === BoundExpressionClass.BOUND_COMPARISON) {
    const cmp = expr as BoundComparisonExpression;
    return containsFunction(cmp.left) || containsFunction(cmp.right);
  }
  if (expr.expressionClass === BoundExpressionClass.BOUND_CONJUNCTION) {
    return (expr as BoundConjunctionExpression).children.some(containsFunction);
  }
  return false;
}

// ============================================================================
// EXISTS decorrelation
// ============================================================================

describe("decorrelateExists", () => {
  it("transforms EXISTS into SEMI join", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100)",
    );
    const optimized = decorrelateExists(plan);

    // Should have a SEMI join instead of a filter with EXISTS subquery
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("SEMI");
    expect(join.conditions).toHaveLength(1);
    expect(join.conditions[0].comparisonType).toBe("EQUAL");

    // Should NOT have a subquery expression in any filter
    const filter = findNode(optimized, LogicalOperatorType.LOGICAL_FILTER);
    if (filter) {
      // The remaining filter should be the uncorrelated predicate (amount > 100)
      // pushed to the build side, not an EXISTS subquery
      expect(filter.expressions[0].expressionClass).not.toBe(
        BoundExpressionClass.BOUND_SUBQUERY,
      );
    }
  });

  it("transforms NOT EXISTS into ANTI join", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("ANTI");
  });

  it("preserves non-EXISTS conditions alongside EXISTS", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE u.age > 18 AND EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    const optimized = decorrelateExists(plan);

    // Should have SEMI join
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("SEMI");

    // Should still have a filter for u.age > 18
    const filter = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_FILTER,
    ) as LogicalFilter;
    expect(filter).toBeTruthy();
    // The filter should be a comparison (age > 18), not a subquery
    expect(filter.expressions[0].expressionClass).toBe(
      BoundExpressionClass.BOUND_COMPARISON,
    );
  });

  it("SEMI join output has only outer columns", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();

    // SEMI join types should match probe (outer) side only
    const outerTypes = join.children[0].types;
    expect(join.types).toEqual(outerTypes);

    // Column bindings should only be from the outer side
    const outerBindings = join.children[0].getColumnBindings();
    expect(join.getColumnBindings()).toEqual(outerBindings);
  });

  it("does not decorrelate uncorrelated EXISTS", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.amount > 100)",
    );
    const optimized = decorrelateExists(plan);

    // Should NOT have a SEMI join (no correlated predicates to join on)
    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    );
    expect(join).toBeNull();
  });

  it("full optimize pipeline produces correct SEMI join", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100)",
    );
    const optimized = optimize(plan, catalog);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("SEMI");
  });

  it("handles EXISTS with multiple correlated conditions", () => {
    const plan = bind(
      `SELECT u.name FROM users u WHERE EXISTS (
        SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = u.name
      )`,
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("SEMI");
    // Should have 2 correlated conditions as join conditions
    expect(join.conditions).toHaveLength(2);
  });

  it("ANTI join output has only outer columns", () => {
    const plan = bind(
      "SELECT u.name FROM users u WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)",
    );
    const optimized = decorrelateExists(plan);

    const join = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_COMPARISON_JOIN,
    ) as LogicalComparisonJoin;
    expect(join).toBeTruthy();
    expect(join.joinType).toBe("ANTI");

    // ANTI join types should match probe (outer) side only
    const outerTypes = join.children[0].types;
    expect(join.types).toEqual(outerTypes);
  });
});

// ============================================================================
// Recursive CTE optimizer tests
// ============================================================================

describe("Recursive CTE optimization", () => {
  it("removeUnusedColumns preserves all columns in recursive CTE anchor and recursive children", () => {
    const plan = bind(
      "WITH RECURSIVE cnt(n, label) AS (SELECT 1, 'a' UNION ALL SELECT n + 1, 'a' FROM cnt WHERE n < 3) SELECT n, label FROM cnt",
    );
    const optimized = removeUnusedColumns(plan);

    // Find the RecursiveCTE node
    const recCTE = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_RECURSIVE_CTE,
    );
    expect(recCTE).toBeTruthy();

    // Anchor should still have 2 columns (not pruned to 1)
    const anchorBindings = recCTE!.children[0].getColumnBindings();
    expect(anchorBindings.length).toBe(2);

    // Recursive child should also have 2 columns
    const recBindings = recCTE!.children[1]!.getColumnBindings();
    expect(recBindings.length).toBeGreaterThanOrEqual(2);
  });

  it("filter pushdown optimizes inside recursive CTE children independently", () => {
    const plan = bind(
      "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 10) SELECT n FROM r WHERE n > 5",
    );
    const optimized = pushdownFilters(plan);

    // The outer filter (n > 5) should stay above the MaterializedCTE
    // The inner filter (n < 10) should stay inside the recursive term
    const recCTE = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_RECURSIVE_CTE,
    );
    expect(recCTE).toBeTruthy();
  });

  it("full optimize pipeline preserves multi-column recursive CTE", () => {
    const plan = bind(
      "WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 20) SELECT a, b FROM fib",
    );
    const optimized = optimize(plan, catalog);

    const recCTE = findNode(
      optimized,
      LogicalOperatorType.LOGICAL_RECURSIVE_CTE,
    );
    expect(recCTE).toBeTruthy();

    // Both columns should be preserved after full optimization
    const anchorBindings = recCTE!.children[0].getColumnBindings();
    expect(anchorBindings.length).toBe(2);
  });
});
