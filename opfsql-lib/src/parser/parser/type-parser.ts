import { BaseParser } from './base.js';
import { TokenType, LogicalType, LogicalTypeId } from '../types.js';

const TYPE_MAP: Record<number, LogicalTypeId> = {
  [TokenType.INTEGER_KW]: LogicalTypeId.INTEGER,
  [TokenType.INT_KW]: LogicalTypeId.INTEGER,
  [TokenType.BIGINT_KW]: LogicalTypeId.BIGINT,
  [TokenType.SMALLINT_KW]: LogicalTypeId.SMALLINT,
  [TokenType.REAL_KW]: LogicalTypeId.FLOAT,
  [TokenType.FLOAT_KW]: LogicalTypeId.FLOAT,
  [TokenType.DOUBLE_KW]: LogicalTypeId.DOUBLE,
  [TokenType.TEXT_KW]: LogicalTypeId.VARCHAR,
  [TokenType.VARCHAR_KW]: LogicalTypeId.VARCHAR,
  [TokenType.CHAR_KW]: LogicalTypeId.VARCHAR,
  [TokenType.BLOB_KW]: LogicalTypeId.BLOB,
  [TokenType.BOOLEAN_KW]: LogicalTypeId.BOOLEAN,
  [TokenType.BOOL_KW]: LogicalTypeId.BOOLEAN,
  [TokenType.JSON_KW]: LogicalTypeId.JSON,
};

export function parseTypeToken(p: BaseParser): LogicalType {
  const token = p.peek();
  const id = TYPE_MAP[token.type];
  if (id !== undefined) {
    p.advance();
    if (p.match(TokenType.LEFT_PAREN)) {
      p.expect(TokenType.INTEGER_LITERAL, `Expected size in type`);
      p.expect(TokenType.RIGHT_PAREN, `Expected ')' after type size`);
    }
    return { id };
  }

  p.error(`Expected type name, got '${token.value || 'EOF'}'`);
}
