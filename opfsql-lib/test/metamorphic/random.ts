import { mulberry32 } from "../support/random.js";

/** Seeded choices, so a failure replays from its seed. */
export class Random {
  private readonly next: () => number;

  constructor(seed: number) {
    this.next = mulberry32(seed);
  }

  chance(probability: number): boolean {
    return this.next() < probability;
  }

  /** An integer from 0 to `count` - 1. */
  below(count: number): number {
    return Math.floor(this.next() * count);
  }

  /** An integer from `low` to `high`, both included. */
  between(low: number, high: number): number {
    return low + this.below(high - low + 1);
  }

  pick<T>(items: readonly T[]): T {
    const item = items[this.below(items.length)];
    if (item === undefined) throw new Error("picked from no items");
    return item;
  }

  /** Up to `count` distinct items, in a random order. */
  some<T>(items: readonly T[], count: number): T[] {
    const left = [...items];
    const chosen: T[] = [];
    while (chosen.length < count && left.length > 0) {
      chosen.push(...left.splice(this.below(left.length), 1));
    }
    return chosen;
  }
}

/** The seed of case `index` of run `seed`: each case replays on its own. */
export function caseSeed(seed: number, index: number): number {
  return Math.imul(seed, 0x9e_37_79_b1) ^ Math.imul(index + 1, 0x85_eb_ca_6b);
}
