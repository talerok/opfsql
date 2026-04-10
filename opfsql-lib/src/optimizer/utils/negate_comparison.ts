import type { ComparisonType } from '../../binder/types.js';

export function negateComparison(type: ComparisonType): ComparisonType {
  switch (type) {
    case 'LESS':
      return 'GREATER_EQUAL';
    case 'GREATER':
      return 'LESS_EQUAL';
    case 'LESS_EQUAL':
      return 'GREATER';
    case 'GREATER_EQUAL':
      return 'LESS';
    case 'EQUAL':
      return 'NOT_EQUAL';
    case 'NOT_EQUAL':
      return 'EQUAL';
  }
}
