import { describe, it, expect } from 'vitest';
import { Lexer } from '../lexer.js';
import { TokenType } from '../types.js';

describe('Lexer', () => {
  function tokenTypes(sql: string): TokenType[] {
    return new Lexer(sql).tokenize().map(t => t.type);
  }

  function tokenValues(sql: string): string[] {
    return new Lexer(sql).tokenize().map(t => t.value);
  }

  it('tokenizes keywords case-insensitively', () => {
    const types = tokenTypes('SELECT select Select');
    expect(types).toEqual([TokenType.SELECT, TokenType.SELECT, TokenType.SELECT, TokenType.EOF]);
  });

  it('tokenizes identifiers', () => {
    const tokens = new Lexer('users age my_table').tokenize();
    expect(tokens[0]).toMatchObject({ type: TokenType.IDENTIFIER, value: 'users' });
    expect(tokens[1]).toMatchObject({ type: TokenType.IDENTIFIER, value: 'age' });
    expect(tokens[2]).toMatchObject({ type: TokenType.IDENTIFIER, value: 'my_table' });
  });

  it('tokenizes quoted identifiers', () => {
    const tokens = new Lexer('"my column"').tokenize();
    expect(tokens[0]).toMatchObject({ type: TokenType.QUOTED_IDENTIFIER, value: 'my column' });
  });

  it('tokenizes string literals', () => {
    const tokens = new Lexer("'hello'").tokenize();
    expect(tokens[0]).toMatchObject({ type: TokenType.STRING_LITERAL, value: 'hello' });
  });

  it('handles escaped quotes in strings', () => {
    const tokens = new Lexer("'it''s'").tokenize();
    expect(tokens[0]).toMatchObject({ type: TokenType.STRING_LITERAL, value: "it's" });
  });

  it('tokenizes integer literals', () => {
    const tokens = new Lexer('42').tokenize();
    expect(tokens[0]).toMatchObject({ type: TokenType.INTEGER_LITERAL, value: '42' });
  });

  it('tokenizes float literals', () => {
    const tokens = new Lexer('3.14').tokenize();
    expect(tokens[0]).toMatchObject({ type: TokenType.FLOAT_LITERAL, value: '3.14' });
  });

  it('tokenizes boolean literals', () => {
    const types = tokenTypes('TRUE FALSE true false');
    expect(types).toEqual([
      TokenType.TRUE_KW, TokenType.FALSE_KW,
      TokenType.TRUE_KW, TokenType.FALSE_KW,
      TokenType.EOF,
    ]);
  });

  it('tokenizes NULL', () => {
    expect(tokenTypes('NULL')).toEqual([TokenType.NULL_KW, TokenType.EOF]);
  });

  it('tokenizes comparison operators', () => {
    const types = tokenTypes('= != <> < <= > >=');
    expect(types).toEqual([
      TokenType.EQUALS, TokenType.NOT_EQUALS, TokenType.NOT_EQUALS,
      TokenType.LESS_THAN, TokenType.LESS_THAN_EQUAL,
      TokenType.GREATER_THAN, TokenType.GREATER_THAN_EQUAL,
      TokenType.EOF,
    ]);
  });

  it('tokenizes arithmetic operators', () => {
    const types = tokenTypes('+ - * / %');
    expect(types).toEqual([
      TokenType.PLUS, TokenType.MINUS, TokenType.STAR, TokenType.SLASH, TokenType.PERCENT,
      TokenType.EOF,
    ]);
  });

  it('tokenizes || as PIPE_PIPE', () => {
    const types = tokenTypes("'a' || 'b'");
    expect(types).toEqual([
      TokenType.STRING_LITERAL, TokenType.PIPE_PIPE, TokenType.STRING_LITERAL,
      TokenType.EOF,
    ]);
  });

  it('single | throws error', () => {
    expect(() => tokenTypes('a | b')).toThrow();
  });

  it('tokenizes punctuation', () => {
    const types = tokenTypes('( ) , ; .');
    expect(types).toEqual([
      TokenType.LEFT_PAREN, TokenType.RIGHT_PAREN, TokenType.COMMA,
      TokenType.SEMICOLON, TokenType.DOT,
      TokenType.EOF,
    ]);
  });

  it('skips single-line comments', () => {
    const types = tokenTypes('SELECT -- this is a comment\n42');
    expect(types).toEqual([TokenType.SELECT, TokenType.INTEGER_LITERAL, TokenType.EOF]);
  });

  it('skips block comments', () => {
    const types = tokenTypes('SELECT /* comment */ 42');
    expect(types).toEqual([TokenType.SELECT, TokenType.INTEGER_LITERAL, TokenType.EOF]);
  });

  it('skips multi-line block comments', () => {
    const types = tokenTypes('SELECT /* line1\nline2 */ 42');
    expect(types).toEqual([TokenType.SELECT, TokenType.INTEGER_LITERAL, TokenType.EOF]);
  });

  it('tracks line and column positions', () => {
    const tokens = new Lexer('SELECT\n  42').tokenize();
    expect(tokens[0]).toMatchObject({ line: 1, column: 1 });
    expect(tokens[1]).toMatchObject({ line: 2, column: 3 });
  });

  it('reports unterminated string', () => {
    expect(() => new Lexer("'hello").tokenize()).toThrow(/Unterminated string/);
  });

  it('reports unterminated block comment', () => {
    expect(() => new Lexer('/* oops').tokenize()).toThrow(/Unterminated block comment/);
  });

  it('reports unsupported keywords', () => {
    expect(() => new Lexer('WINDOW').tokenize()).toThrow(/not supported/);
    expect(() => new Lexer('PARTITION').tokenize()).toThrow(/not supported/);
    expect(() => new Lexer('OVER').tokenize()).toThrow(/not supported/);
    expect(() => new Lexer('TRUNCATE').tokenize()).toThrow(/not supported/);
  });

  it('tokenizes type keywords', () => {
    const types = tokenTypes('INTEGER TEXT BOOLEAN VARCHAR');
    expect(types).toEqual([
      TokenType.INTEGER_KW, TokenType.TEXT_KW, TokenType.BOOLEAN_KW, TokenType.VARCHAR_KW,
      TokenType.EOF,
    ]);
  });

  it('tokenizes a full SELECT statement', () => {
    const types = tokenTypes("SELECT id, name FROM users WHERE age > 18");
    expect(types).toEqual([
      TokenType.SELECT, TokenType.IDENTIFIER, TokenType.COMMA, TokenType.IDENTIFIER,
      TokenType.FROM, TokenType.IDENTIFIER, TokenType.WHERE, TokenType.IDENTIFIER,
      TokenType.GREATER_THAN, TokenType.INTEGER_LITERAL, TokenType.EOF,
    ]);
  });
});
