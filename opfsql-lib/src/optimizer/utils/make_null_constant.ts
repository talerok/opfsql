import type {
  BoundConstantExpression,
  LogicalType,
} from '../../binder/types.js';
import { makeConstant } from './make_constant.js';

export function makeNullConstant(returnType: LogicalType = 'NULL'): BoundConstantExpression {
  return makeConstant(null, returnType);
}
