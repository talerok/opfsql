import type { LogicalType } from '../../binder/types.js';

export function isNumericType(type: LogicalType): boolean {
  return type === 'INTEGER' || type === 'BIGINT' || type === 'REAL';
}
