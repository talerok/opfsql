export { startAggregate } from "./aggregate.js";
export { commonType, staticType } from "./arguments.js";
export {
  aggregateFunction,
  isAggregateFunction,
  lookupFunction,
  resolveCall,
  scalarFunction,
} from "./registry.js";
export type {
  Accumulator,
  AggregateFunction,
  Argument,
  ArgumentKind,
  CallSite,
  Implementation,
  ScalarFunction,
  Signature,
  SqlFunction,
  StaticType,
} from "./types.js";
