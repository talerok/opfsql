import type { BoundConstantExpression } from '../../binder/types.js';
import { makeConstant } from './make_constant.js';

export function makeBoolConstant(value: boolean): BoundConstantExpression {
  return makeConstant(value, 'BOOLEAN');
}
