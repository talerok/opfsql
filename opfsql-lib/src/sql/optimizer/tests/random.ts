import type { Value } from "../../../values/index.js";
import type { StoredRow, Tables } from "./scans.js";

// Seeded randomness, so a failing case runs again the same way, and sample
// tables for users and orders (builders.ts) with NULLs, mixed kinds in the
// ANY column, and values on both sides of the seeks the tests write.

const MODULUS = 0x7f_ff_ff_ff;
const MULTIPLIER = 1_103_515_245;
const INCREMENT = 12_345;

export class Random {
  private state: number;

  constructor(seed: number) {
    this.state = seed;
  }

  /** A number in [0, 1). */
  next(): number {
    this.state = (this.state * MULTIPLIER + INCREMENT) % MODULUS;
    return this.state / MODULUS;
  }

  /** A whole number in [0, count). */
  below(count: number): number {
    return Math.floor(this.next() * count);
  }

  pick<T>(items: readonly T[]): T {
    const item = items[this.below(items.length)];
    if (item === undefined) throw new RangeError("nothing to pick from");
    return item;
  }

  /** True with probability `chance`. */
  chance(chance: number): boolean {
    return this.next() < chance;
  }
}

const NAMES: readonly Value[] = [
  null,
  "a",
  "b",
  "bob",
  "Bob",
  "BOB",
  "5",
  "",
  "z",
  "é",
];
const AGES: readonly Value[] = [null, -3, 0, 1, 5, 18, 30, 65, 100];
const TAGS: readonly Value[] = [
  null,
  5,
  "5",
  "x",
  1.5,
  true,
  false,
  "05",
  0,
  -1,
  "X",
  1,
  "1",
];
const USER_IDS: readonly Value[] = [null, 1, 2, 3, 5, 8, 13, 40, 99];
const TOTALS: readonly Value[] = [null, 0, 1.5, 5, 10, -2];
const STATUSES: readonly Value[] = ["paid", "open", null, "5"];
const PAID: readonly Value[] = [true, false, null];

/** users(id, name, age, email, tag) and orders(id, user_id, total, status, paid). */
export function sampleTables(random: Random): Tables {
  const users: StoredRow[] = [];
  for (let id = 1; id <= 40; id += 1) {
    const email = `e${String(id % 37)}x${String(id)}`;
    const values = [
      id,
      random.pick(NAMES),
      random.pick(AGES),
      email,
      random.pick(TAGS),
    ];
    users.push({ rowid: id, values });
  }
  const orders: StoredRow[] = [];
  for (let id = 1; id <= 30; id += 1) {
    const values = [
      id,
      random.pick(USER_IDS),
      random.pick(TOTALS),
      random.pick(STATUSES),
      random.pick(PAID),
    ];
    orders.push({ rowid: id, values });
  }
  return new Map([
    ["users", users],
    ["orders", orders],
  ]);
}
