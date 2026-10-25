import { BaseParser } from "./base.js";
import {
  TokenType,
  StatementType,
  TransactionStatement,
  TransactionType,
} from "../types.js";

export function parseTransaction(
  p: BaseParser,
  txType: TransactionType,
): TransactionStatement {
  p.advance(); // BEGIN, COMMIT, or ROLLBACK
  const mode = txType === TransactionType.BEGIN ? parseBeginMode(p) : undefined;
  p.match(TokenType.TRANSACTION);
  return {
    type: StatementType.TRANSACTION_STATEMENT,
    transaction_type: txType,
    ...(mode !== undefined && { mode }),
  };
}

/** BEGIN DEFERRED | IMMEDIATE: the words are not keywords anywhere else. */
function parseBeginMode(p: BaseParser): TransactionStatement["mode"] {
  const token = p.peek();
  if (token.type !== TokenType.IDENTIFIER) return undefined;
  const word = token.value.toUpperCase();
  if (word !== "DEFERRED" && word !== "IMMEDIATE") return undefined;
  p.advance();
  return word;
}
