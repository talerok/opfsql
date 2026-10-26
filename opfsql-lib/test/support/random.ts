/** Seeded PRNG (mulberry32): the same seed always yields the same sequence. */
export function mulberry32(seed: number): Random {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d_2b_79_f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296;
  };
}

/** A source of numbers in [0, 1), like Math.random or a seeded mulberry32. */
export type Random = () => number;

/** One of `items`, picked by `random`; there must be one. */
export function pick<T>(random: Random, items: readonly T[]): T {
  const item = items[Math.floor(random() * items.length)];
  if (item === undefined) throw new Error("nothing to pick from");
  return item;
}
