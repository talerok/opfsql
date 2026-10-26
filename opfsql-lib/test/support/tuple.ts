/**
 * A tuple of `N` items of `T`: what a helper that makes `count` of something
 * returns, so `const [a, b] = await openTabs(name, 2)` types both.
 */
export type TupleOf<
  T,
  N extends number,
  Built extends T[] = [],
> = Built["length"] extends N ? Built : TupleOf<T, N, [...Built, T]>;
