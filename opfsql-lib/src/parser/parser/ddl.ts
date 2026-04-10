import { BaseParser } from './base.js';
import { parseUnary } from './expressions.js';
import { parseTypeToken } from './type-parser.js';
import {
  TokenType, ParsedExpression,
  StatementType, Statement,
  CreateTableStatement, CreateIndexStatement,
  AlterTableStatement, AlterType,
  DropStatement, DropType,
  ColumnDefinition, ForeignKeyConstraint,
} from '../types.js';

export function parseCreate(p: BaseParser): Statement {
  p.expect(TokenType.CREATE);

  if (p.check(TokenType.UNIQUE)) {
    p.advance();
    return parseCreateIndex(p, true);
  }

  if (p.check(TokenType.INDEX)) {
    return parseCreateIndex(p, false);
  }

  if (p.check(TokenType.TABLE)) {
    return parseCreateTable(p);
  }

  p.error(`Expected TABLE or INDEX after CREATE, got '${p.peek().value || 'EOF'}'`);
}

function parseCreateTable(p: BaseParser): CreateTableStatement {
  p.expect(TokenType.TABLE);

  let if_not_exists = false;
  if (p.match(TokenType.IF)) {
    p.expect(TokenType.NOT, `Expected NOT after IF`);
    p.expect(TokenType.EXISTS, `Expected EXISTS after IF NOT`);
    if_not_exists = true;
  }

  const table = p.expect(TokenType.IDENTIFIER, `Expected table name`).value;
  p.expect(TokenType.LEFT_PAREN, `Expected '(' after table name`);

  const columns: ColumnDefinition[] = [];
  const primary_key: string[] = [];
  const foreign_keys: ForeignKeyConstraint[] = [];

  do {
    // Table-level PRIMARY KEY
    if (p.check(TokenType.PRIMARY)) {
      p.advance();
      p.expect(TokenType.KEY, `Expected KEY after PRIMARY`);
      p.expect(TokenType.LEFT_PAREN);
      do {
        primary_key.push(p.expect(TokenType.IDENTIFIER, `Expected column name`).value);
      } while (p.match(TokenType.COMMA));
      p.expect(TokenType.RIGHT_PAREN);
      continue;
    }

    // Table-level FOREIGN KEY
    if (p.check(TokenType.FOREIGN)) {
      p.advance();
      p.expect(TokenType.KEY, `Expected KEY after FOREIGN`);
      p.expect(TokenType.LEFT_PAREN);
      const fkColumns: string[] = [];
      do {
        fkColumns.push(p.expect(TokenType.IDENTIFIER, `Expected column name`).value);
      } while (p.match(TokenType.COMMA));
      p.expect(TokenType.RIGHT_PAREN);
      p.expect(TokenType.REFERENCES, `Expected REFERENCES after FOREIGN KEY columns`);
      const refTable = p.expect(TokenType.IDENTIFIER, `Expected referenced table name`).value;
      p.expect(TokenType.LEFT_PAREN);
      const refColumns: string[] = [];
      do {
        refColumns.push(p.expect(TokenType.IDENTIFIER, `Expected column name`).value);
      } while (p.match(TokenType.COMMA));
      p.expect(TokenType.RIGHT_PAREN);
      foreign_keys.push({ columns: fkColumns, ref_table: refTable, ref_columns: refColumns });
      continue;
    }

    columns.push(parseColumnDefinition(p));
  } while (p.match(TokenType.COMMA));

  p.expect(TokenType.RIGHT_PAREN, `Expected ')' after column definitions`);

  return {
    type: StatementType.CREATE_TABLE_STATEMENT,
    table,
    if_not_exists,
    columns,
    primary_key,
    foreign_keys,
  };
}

function parseColumnDefinition(p: BaseParser): ColumnDefinition {
  const name = p.expect(TokenType.IDENTIFIER, `Expected column name`).value;
  const colType = parseTypeToken(p);

  let is_primary_key = false;
  let is_not_null = false;
  let is_unique = false;
  let default_value: ParsedExpression | null = null;

  while (true) {
    if (p.check(TokenType.PRIMARY)) {
      p.advance();
      p.expect(TokenType.KEY, `Expected KEY after PRIMARY`);
      is_primary_key = true;
      continue;
    }
    if (p.check(TokenType.NOT)) {
      p.advance();
      p.expect(TokenType.NULL_KW, `Expected NULL after NOT`);
      is_not_null = true;
      continue;
    }
    if (p.check(TokenType.UNIQUE)) {
      p.advance();
      is_unique = true;
      continue;
    }
    if (p.check(TokenType.DEFAULT)) {
      p.advance();
      default_value = parseUnary(p);
      continue;
    }
    break;
  }

  return {
    name,
    type: colType,
    is_primary_key,
    is_not_null,
    is_unique,
    default_value,
  };
}

function parseCreateIndex(p: BaseParser, is_unique: boolean): CreateIndexStatement {
  p.expect(TokenType.INDEX);

  let if_not_exists = false;
  if (p.match(TokenType.IF)) {
    p.expect(TokenType.NOT, `Expected NOT after IF`);
    p.expect(TokenType.EXISTS, `Expected EXISTS after IF NOT`);
    if_not_exists = true;
  }

  const index_name = p.expect(TokenType.IDENTIFIER, `Expected index name`).value;
  p.expect(TokenType.ON, `Expected ON after index name`);
  const table_name = p.expect(TokenType.IDENTIFIER, `Expected table name after ON`).value;

  p.expect(TokenType.LEFT_PAREN);
  const columns: string[] = [];
  do {
    columns.push(p.expect(TokenType.IDENTIFIER, `Expected column name`).value);
  } while (p.match(TokenType.COMMA));
  p.expect(TokenType.RIGHT_PAREN);

  return {
    type: StatementType.CREATE_INDEX_STATEMENT,
    index_name,
    table_name,
    columns,
    is_unique,
    if_not_exists,
  };
}

export function parseAlterTable(p: BaseParser): AlterTableStatement {
  p.expect(TokenType.ALTER);
  p.expect(TokenType.TABLE, `Expected TABLE after ALTER`);
  const table = p.expect(TokenType.IDENTIFIER, `Expected table name after ALTER TABLE`).value;

  if (p.match(TokenType.ADD)) {
    p.match(TokenType.COLUMN);
    const column_def = parseColumnDefinition(p);
    return {
      type: StatementType.ALTER_TABLE_STATEMENT,
      table,
      alter_type: AlterType.ADD_COLUMN,
      column_def,
      column_name: null,
    };
  }

  if (p.match(TokenType.DROP)) {
    p.match(TokenType.COLUMN);
    const column_name = p.expect(TokenType.IDENTIFIER, `Expected column name to drop`).value;
    return {
      type: StatementType.ALTER_TABLE_STATEMENT,
      table,
      alter_type: AlterType.DROP_COLUMN,
      column_def: null,
      column_name,
    };
  }

  p.error(`Expected ADD or DROP after ALTER TABLE ${table}`);
}

export function parseDrop(p: BaseParser): DropStatement {
  p.expect(TokenType.DROP);

  let drop_type: DropType;
  if (p.match(TokenType.TABLE)) {
    drop_type = DropType.TABLE;
  } else if (p.match(TokenType.INDEX)) {
    drop_type = DropType.INDEX;
  } else {
    p.error(`Expected TABLE or INDEX after DROP, got '${p.peek().value || 'EOF'}'`);
  }

  let if_exists = false;
  if (p.match(TokenType.IF)) {
    p.expect(TokenType.EXISTS, `Expected EXISTS after IF`);
    if_exists = true;
  }

  const name = p.expect(TokenType.IDENTIFIER, `Expected name after DROP ${DropType[drop_type]}`).value;

  return {
    type: StatementType.DROP_STATEMENT,
    drop_type,
    name,
    if_exists,
  };
}
