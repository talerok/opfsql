import type { ComparisonType } from '../../binder/types.js';

export function flipComparison(type: ComparisonType): ComparisonType {
  switch (type) {
    case 'LESS':
      return 'GREATER';
    case 'GREATER':
      return 'LESS';
    case 'LESS_EQUAL':
      return 'GREATER_EQUAL';
    case 'GREATER_EQUAL':
      return 'LESS_EQUAL';
    case 'EQUAL':
      return 'EQUAL';
    case 'NOT_EQUAL':
      return 'NOT_EQUAL';
  }
}
