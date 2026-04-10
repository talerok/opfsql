import { BaseParser } from './base.js';
import {
  TokenType, StatementType,
  TransactionStatement, TransactionType,
} from '../types.js';

export function parseTransaction(p: BaseParser, txType: TransactionType): TransactionStatement {
  p.advance(); // BEGIN, COMMIT, or ROLLBACK
  p.match(TokenType.TRANSACTION);
  return {
    type: StatementType.TRANSACTION_STATEMENT,
    transaction_type: txType,
  };
}
