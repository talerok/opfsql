import { BaseParser } from './base.js';
import {
  TokenType, LogicalTypeId,
  ExpressionClass, ExpressionType, ParsedExpression,
  ConjunctionExpression, OperatorExpression, BetweenExpression,
  FunctionExpression, SubqueryExpression, CaseExpression,
  CastExpression,
  SelectStatement,
  ParseError,
  type JsonPathSegment,
} from '../types.js';
import { parseTypeToken } from './type-parser.js';

const COMPARISON_OPS: Record<number, ExpressionType> = {
  [TokenType.EQUALS]: ExpressionType.COMPARE_EQUAL,
  [TokenType.NOT_EQUALS]: ExpressionType.COMPARE_NOTEQUAL,
  [TokenType.LESS_THAN]: ExpressionType.COMPARE_LESSTHAN,
  [TokenType.LESS_THAN_EQUAL]: ExpressionType.COMPARE_LESSTHANOREQUALTO,
  [TokenType.GREATER_THAN]: ExpressionType.COMPARE_GREATERTHAN,
  [TokenType.GREATER_THAN_EQUAL]: ExpressionType.COMPARE_GREATERTHANOREQUALTO,
};

export function parseExpression(p: BaseParser): ParsedExpression {
  return parseOr(p);
}

function parseOr(p: BaseParser): ParsedExpression {
  let left = parseAnd(p);

  if (p.match(TokenType.OR)) {
    const children: ParsedExpression[] = [left];
    do {
      children.push(parseAnd(p));
    } while (p.match(TokenType.OR));
    left = {
      expression_class: ExpressionClass.CONJUNCTION,
      alias: null,
      type: ExpressionType.CONJUNCTION_OR,
      children,
    } satisfies ConjunctionExpression;
  }

  return left;
}

function parseAnd(p: BaseParser): ParsedExpression {
  let left = parseNot(p);

  if (p.match(TokenType.AND)) {
    const children: ParsedExpression[] = [left];
    do {
      children.push(parseNot(p));
    } while (p.match(TokenType.AND));
    left = {
      expression_class: ExpressionClass.CONJUNCTION,
      alias: null,
      type: ExpressionType.CONJUNCTION_AND,
      children,
    } satisfies ConjunctionExpression;
  }

  return left;
}

function parseNot(p: BaseParser): ParsedExpression {
  if (p.match(TokenType.NOT)) {
    if (p.check(TokenType.EXISTS)) {
      return parseExistsExpression(p, true);
    }
    const expr = parseNot(p);
    return {
      expression_class: ExpressionClass.OPERATOR,
      alias: null,
      type: ExpressionType.OPERATOR_NOT,
      children: [expr],
    } satisfies OperatorExpression;
  }

  if (p.check(TokenType.EXISTS)) {
    return parseExistsExpression(p, false);
  }

  return parseComparison(p);
}

function parseExistsExpression(p: BaseParser, negated: boolean): SubqueryExpression {
  p.expect(TokenType.EXISTS);
  p.expect(TokenType.LEFT_PAREN, `Expected '(' after EXISTS`);
  const subquery = p.parseSelectStatement() as SelectStatement;
  p.expect(TokenType.RIGHT_PAREN, `Expected ')' after EXISTS subquery`);
  return {
    expression_class: ExpressionClass.SUBQUERY,
    alias: null,
    subquery_type: negated ? 'NOT_EXISTS' : 'EXISTS',
    subquery,
    child: null,
  };
}

function parseComparison(p: BaseParser): ParsedExpression {
  let left = parseConcat(p);

  // IS [NOT] NULL
  if (p.check(TokenType.IS)) {
    p.advance();
    if (p.match(TokenType.NOT)) {
      p.expect(TokenType.NULL_KW, `Expected NULL after IS NOT`);
      return {
        expression_class: ExpressionClass.OPERATOR,
        alias: null,
        type: ExpressionType.OPERATOR_IS_NOT_NULL,
        children: [left],
      };
    }
    p.expect(TokenType.NULL_KW, `Expected NULL after IS`);
    return {
      expression_class: ExpressionClass.OPERATOR,
      alias: null,
      type: ExpressionType.OPERATOR_IS_NULL,
      children: [left],
    };
  }

  // NOT BETWEEN / NOT LIKE / NOT IN
  if (p.check(TokenType.NOT)) {
    const next = p.peekAt(1);

    if (next.type === TokenType.BETWEEN) {
      p.advance(); // NOT
      p.advance(); // BETWEEN
      const lower = parseAddSub(p);
      p.expect(TokenType.AND, `Expected AND in BETWEEN expression`);
      const upper = parseAddSub(p);
      const between: BetweenExpression = {
        expression_class: ExpressionClass.BETWEEN,
        alias: null,
        input: left,
        lower,
        upper,
      };
      return {
        expression_class: ExpressionClass.OPERATOR,
        alias: null,
        type: ExpressionType.OPERATOR_NOT,
        children: [between],
      };
    }

    if (next.type === TokenType.LIKE) {
      p.advance(); // NOT
      p.advance(); // LIKE
      const right = parseAddSub(p);
      return {
        expression_class: ExpressionClass.COMPARISON,
        alias: null,
        type: ExpressionType.COMPARE_NOT_LIKE,
        left,
        right,
      };
    }

    if (next.type === TokenType.IN) {
      p.advance(); // NOT
      p.advance(); // IN
      return parseInExpression(p, left, true);
    }
  }

  // BETWEEN
  if (p.match(TokenType.BETWEEN)) {
    const lower = parseAddSub(p);
    p.expect(TokenType.AND, `Expected AND in BETWEEN expression`);
    const upper = parseAddSub(p);
    return {
      expression_class: ExpressionClass.BETWEEN,
      alias: null,
      input: left,
      lower,
      upper,
    };
  }

  // LIKE
  if (p.match(TokenType.LIKE)) {
    const right = parseAddSub(p);
    return {
      expression_class: ExpressionClass.COMPARISON,
      alias: null,
      type: ExpressionType.COMPARE_LIKE,
      left,
      right,
    };
  }

  // IN
  if (p.match(TokenType.IN)) {
    return parseInExpression(p, left, false);
  }

  const opType = COMPARISON_OPS[p.peek().type];
  if (opType !== undefined) {
    p.advance();
    const right = parseAddSub(p);
    return {
      expression_class: ExpressionClass.COMPARISON,
      alias: null,
      type: opType,
      left,
      right,
    };
  }

  return left;
}

function parseInExpression(p: BaseParser, left: ParsedExpression, negated: boolean): ParsedExpression {
  p.expect(TokenType.LEFT_PAREN, `Expected '(' after IN`);

  // IN (SELECT ...)
  if (p.check(TokenType.SELECT) || p.check(TokenType.WITH)) {
    const subquery = p.parseSelectStatement() as SelectStatement;
    p.expect(TokenType.RIGHT_PAREN, `Expected ')' after IN subquery`);
    const anyExpr: SubqueryExpression = {
      expression_class: ExpressionClass.SUBQUERY,
      alias: null,
      subquery_type: 'ANY',
      subquery,
      child: left,
      comparison_type: ExpressionType.COMPARE_EQUAL,
    };
    if (negated) {
      return {
        expression_class: ExpressionClass.OPERATOR,
        alias: null,
        type: ExpressionType.OPERATOR_NOT,
        children: [anyExpr],
      };
    }
    return anyExpr;
  }

  // IN (val1, val2, ...)
  const values: ParsedExpression[] = [left];
  do {
    values.push(parseExpression(p));
  } while (p.match(TokenType.COMMA));
  p.expect(TokenType.RIGHT_PAREN, `Expected ')' after IN list`);

  return {
    expression_class: ExpressionClass.OPERATOR,
    alias: null,
    type: negated ? ExpressionType.OPERATOR_NOT_IN : ExpressionType.OPERATOR_IN,
    children: values,
  };
}

function parseConcat(p: BaseParser): ParsedExpression {
  let left = parseAddSub(p);

  while (p.check(TokenType.PIPE_PIPE)) {
    p.advance();
    const right = parseAddSub(p);
    left = {
      expression_class: ExpressionClass.OPERATOR,
      alias: null,
      type: ExpressionType.OPERATOR_CONCAT,
      children: [left, right],
    };
  }

  return left;
}

function parseAddSub(p: BaseParser): ParsedExpression {
  let left = parseMulDiv(p);

  while (p.check(TokenType.PLUS) || p.check(TokenType.MINUS)) {
    const op = p.advance();
    const right = parseMulDiv(p);
    const opType = op.type === TokenType.PLUS
      ? ExpressionType.OPERATOR_ADD
      : ExpressionType.OPERATOR_SUBTRACT;
    left = {
      expression_class: ExpressionClass.OPERATOR,
      alias: null,
      type: opType,
      children: [left, right],
    };
  }

  return left;
}

function parseMulDiv(p: BaseParser): ParsedExpression {
  let left = parseUnary(p);

  while (p.check(TokenType.STAR) || p.check(TokenType.SLASH) || p.check(TokenType.PERCENT)) {
    const op = p.advance();
    const right = parseUnary(p);
    let opType: ExpressionType;
    if (op.type === TokenType.STAR) opType = ExpressionType.OPERATOR_MULTIPLY;
    else if (op.type === TokenType.SLASH) opType = ExpressionType.OPERATOR_DIVIDE;
    else opType = ExpressionType.OPERATOR_MOD;
    left = {
      expression_class: ExpressionClass.OPERATOR,
      alias: null,
      type: opType,
      children: [left, right],
    };
  }

  return left;
}

export function parseUnary(p: BaseParser): ParsedExpression {
  if (p.match(TokenType.MINUS)) {
    const expr = parseUnary(p);
    return {
      expression_class: ExpressionClass.OPERATOR,
      alias: null,
      type: ExpressionType.OPERATOR_NEGATE,
      children: [expr],
    };
  }
  if (p.match(TokenType.PLUS)) {
    return parseUnary(p);
  }
  return parsePrimary(p);
}

export function parsePrimary(p: BaseParser): ParsedExpression {
  const token = p.peek();

  if (p.check(TokenType.INTEGER_LITERAL)) {
    p.advance();
    return {
      expression_class: ExpressionClass.CONSTANT,
      alias: null,
      value: {
        type: { id: LogicalTypeId.INTEGER },
        is_null: false,
        value: parseInt(token.value, 10),
      },
    };
  }

  if (p.check(TokenType.FLOAT_LITERAL)) {
    p.advance();
    return {
      expression_class: ExpressionClass.CONSTANT,
      alias: null,
      value: {
        type: { id: LogicalTypeId.DOUBLE },
        is_null: false,
        value: parseFloat(token.value),
      },
    };
  }

  if (p.check(TokenType.STRING_LITERAL)) {
    p.advance();
    return {
      expression_class: ExpressionClass.CONSTANT,
      alias: null,
      value: {
        type: { id: LogicalTypeId.VARCHAR },
        is_null: false,
        value: token.value,
      },
    };
  }

  if (p.check(TokenType.BLOB_LITERAL)) {
    p.advance();
    const hex = token.value;
    const bytes = new Uint8Array(hex.length / 2);
    for (let i = 0; i < hex.length; i += 2) {
      bytes[i / 2] = parseInt(hex.substring(i, i + 2), 16);
    }
    return {
      expression_class: ExpressionClass.CONSTANT,
      alias: null,
      value: {
        type: { id: LogicalTypeId.BLOB },
        is_null: false,
        value: bytes,
      },
    };
  }

  if (p.check(TokenType.TRUE_KW)) {
    p.advance();
    return {
      expression_class: ExpressionClass.CONSTANT,
      alias: null,
      value: { type: { id: LogicalTypeId.BOOLEAN }, is_null: false, value: true },
    };
  }

  if (p.check(TokenType.FALSE_KW)) {
    p.advance();
    return {
      expression_class: ExpressionClass.CONSTANT,
      alias: null,
      value: { type: { id: LogicalTypeId.BOOLEAN }, is_null: false, value: false },
    };
  }

  if (p.check(TokenType.NULL_KW)) {
    p.advance();
    return {
      expression_class: ExpressionClass.CONSTANT,
      alias: null,
      value: { type: { id: LogicalTypeId.INTEGER }, is_null: true, value: null },
    };
  }

  if (p.check(TokenType.PARAMETER)) {
    p.advance();
    const index = parseInt(token.value, 10) - 1; // $1 → index 0
    if (index < 0) {
      throw new ParseError(`Parameter index must be >= 1`, token.line, token.column, token);
    }
    return {
      expression_class: ExpressionClass.PARAMETER,
      alias: null,
      index,
    };
  }

  if (p.check(TokenType.CASE)) {
    return parseCaseExpression(p);
  }

  if (p.check(TokenType.CAST)) {
    return parseCastExpression(p);
  }

  if (p.check(TokenType.STAR)) {
    p.advance();
    return {
      expression_class: ExpressionClass.STAR,
      alias: null,
      table_name: null,
    };
  }

  // Parenthesized expression or scalar subquery
  if (p.check(TokenType.LEFT_PAREN)) {
    p.advance();
    if (p.check(TokenType.SELECT) || p.check(TokenType.WITH)) {
      const subquery = p.parseSelectStatement() as SelectStatement;
      p.expect(TokenType.RIGHT_PAREN, `Expected ')' after subquery`);
      return {
        expression_class: ExpressionClass.SUBQUERY,
        alias: null,
        subquery_type: 'SCALAR',
        subquery,
        child: null,
      };
    }
    const expr = parseExpression(p);
    p.expect(TokenType.RIGHT_PAREN, `Expected ')'`);
    return expr;
  }

  if (
    p.check(TokenType.IDENTIFIER) ||
    p.check(TokenType.QUOTED_IDENTIFIER) ||
    p.check(TokenType.EXCLUDED)
  ) {
    return parseColumnOrFunction(p);
  }

  p.error(`Expected expression, got '${token.value || 'EOF'}'`);
}

function parseColumnOrFunction(p: BaseParser): ParsedExpression {
  const name = p.advance();
  const ident = name.value;

  // Function call
  if (p.check(TokenType.LEFT_PAREN)) {
    return parseFunctionCall(p, ident);
  }

  // Qualified name: table.column, table.*, or N-level dot path for JSON
  if (p.match(TokenType.DOT)) {
    if (p.check(TokenType.STAR)) {
      p.advance();
      return {
        expression_class: ExpressionClass.STAR,
        alias: null,
        table_name: ident,
      };
    }

    const col = p.expectIdentifier(`Expected column name after '${ident}.'`);
    const names = [ident, col.value];

    // Continue collecting dot-separated identifiers (for JSON path: t.col.field1.field2)
    while (p.check(TokenType.DOT) && p.peekAt(1).type !== TokenType.STAR) {
      p.advance(); // consume DOT
      const next = p.expectIdentifier(`Expected field name after '.'`);
      names.push(next.value);
    }

    // Collect bracket access segments (for JSON array: col.items[0].name)
    const pathSegments = parseBracketPath(p);

    return {
      expression_class: ExpressionClass.COLUMN_REF,
      alias: null,
      column_names: names,
      ...(pathSegments.length > 0 && { path: pathSegments }),
    };
  }

  // Single identifier — check for bracket access (e.g., col[0])
  const pathSegments = parseBracketPath(p);
  if (pathSegments.length > 0) {
    return {
      expression_class: ExpressionClass.COLUMN_REF,
      alias: null,
      column_names: [ident],
      path: pathSegments,
    };
  }

  return {
    expression_class: ExpressionClass.COLUMN_REF,
    alias: null,
    column_names: [ident],
  };
}

/** Parse bracket/dot path segments: [0].field[1].field2 ... */
function parseBracketPath(p: BaseParser): JsonPathSegment[] {
  const segments: JsonPathSegment[] = [];
  while (p.check(TokenType.LEFT_BRACKET) || p.check(TokenType.DOT)) {
    if (p.match(TokenType.LEFT_BRACKET)) {
      const idx = p.expect(TokenType.INTEGER_LITERAL, `Expected integer index in bracket access`);
      p.expect(TokenType.RIGHT_BRACKET, `Expected ']' after bracket index`);
      segments.push({ type: 'index', value: parseInt(idx.value, 10) });
    } else {
      p.advance(); // consume DOT
      const field = p.expectIdentifier(`Expected field name after '.'`);
      segments.push({ type: 'field', name: field.value });
    }
  }
  return segments;
}

function parseFunctionCall(p: BaseParser, name: string): FunctionExpression {
  p.expect(TokenType.LEFT_PAREN);
  const funcName = name.toLowerCase();

  if (p.check(TokenType.STAR) && funcName === 'count') {
    p.advance();
    p.expect(TokenType.RIGHT_PAREN, `Expected ')' after COUNT(*)`);
    return {
      expression_class: ExpressionClass.FUNCTION,
      alias: null,
      function_name: funcName,
      children: [],
      distinct: false,
      is_star: true,
    };
  }

  if (p.check(TokenType.RIGHT_PAREN)) {
    p.advance();
    return {
      expression_class: ExpressionClass.FUNCTION,
      alias: null,
      function_name: funcName,
      children: [],
      distinct: false,
      is_star: false,
    };
  }

  const distinct = p.match(TokenType.DISTINCT);

  const children: ParsedExpression[] = [];
  do {
    children.push(parseExpression(p));
  } while (p.match(TokenType.COMMA));

  p.expect(TokenType.RIGHT_PAREN, `Expected ')' after function arguments`);

  return {
    expression_class: ExpressionClass.FUNCTION,
    alias: null,
    function_name: funcName,
    children,
    distinct,
    is_star: false,
  };
}

function parseCaseExpression(p: BaseParser): CaseExpression {
  p.expect(TokenType.CASE);

  let operand: ParsedExpression | null = null;
  if (!p.check(TokenType.WHEN) && !p.check(TokenType.END)) {
    operand = parseExpression(p);
  }

  const case_checks: Array<{ when_expr: ParsedExpression; then_expr: ParsedExpression }> = [];

  while (p.match(TokenType.WHEN)) {
    let when_expr = parseExpression(p);
    if (operand !== null) {
      when_expr = {
        expression_class: ExpressionClass.COMPARISON,
        alias: null,
        type: ExpressionType.COMPARE_EQUAL,
        left: operand,
        right: when_expr,
      };
    }
    p.expect(TokenType.THEN, `Expected THEN after WHEN condition`);
    const then_expr = parseExpression(p);
    case_checks.push({ when_expr, then_expr });
  }

  let else_expr: ParsedExpression | null = null;
  if (p.match(TokenType.ELSE)) {
    else_expr = parseExpression(p);
  }

  p.expect(TokenType.END, `Expected END after CASE expression`);

  return {
    expression_class: ExpressionClass.CASE,
    alias: null,
    case_checks,
    else_expr,
  };
}

function parseCastExpression(p: BaseParser): CastExpression {
  p.expect(TokenType.CAST);
  p.expect(TokenType.LEFT_PAREN, `Expected '(' after CAST`);
  const child = parseExpression(p);
  p.expect(TokenType.AS, `Expected AS in CAST expression`);
  const cast_type = parseTypeToken(p);
  p.expect(TokenType.RIGHT_PAREN, `Expected ')' after CAST expression`);

  return {
    expression_class: ExpressionClass.CAST,
    alias: null,
    child,
    cast_type,
  };
}
