import type { Value } from "../../src/values/index.js";

// Random SQL values for property tests, biased toward the cases encodings get
// wrong: code points that UTF-16 and UTF-8 order differently, NUL characters
// and zero bytes, integers at every width boundary, and JSON objects whose
// keys are out of order.

/** Code points from every range that UTF-16 and UTF-8 order differently. */
const CODE_POINTS = [
  0x00, 0x41, 0x61, 0x7f, 0xe9, 0x4_00, 0xd7_ff, 0xe0_00, 0xff_fd, 0x1_00_00,
  0x1_f6_00, 0x10_ff_ff,
];

const NUMBERS = [
  0,
  1,
  -1,
  127,
  128,
  -129,
  32_767,
  32_768,
  -8_388_609,
  2_147_483_648,
  -2_147_483_649,
  2 ** 47,
  -(2 ** 47) - 1,
  Number.MAX_SAFE_INTEGER,
  Number.MIN_SAFE_INTEGER,
  0.5,
  -0.5,
  2.25,
  1e-300,
  -1e300,
  Infinity,
  -Infinity,
];

export function randomText(random: () => number, maxLength = 4): string {
  const length = Math.floor(random() * (maxLength + 1));
  return Array.from({ length }, () =>
    String.fromCodePoint(pick(random, CODE_POINTS)),
  ).join("");
}

export function randomValue(random: () => number): Value {
  switch (Math.floor(random() * 9)) {
    case 0: {
      return null;
    }
    case 1: {
      return random() < 0.5;
    }
    case 2: {
      return Math.floor(random() * 7) - 3;
    }
    case 3: {
      return pick(random, NUMBERS);
    }
    case 4:
    case 5: {
      return randomText(random);
    }
    case 6: {
      return Uint8Array.from({ length: Math.floor(random() * 3) }, () =>
        Math.floor(random() * 3),
      );
    }
    case 7: {
      return { b: Math.floor(random() * 2), a: randomText(random) };
    }
    default: {
      return [Math.floor(random() * 2), randomText(random)];
    }
  }
}

function pick<T>(random: () => number, items: readonly T[]): T {
  const item = items[Math.floor(random() * items.length)];
  if (item === undefined) throw new Error("pick from an empty list");
  return item;
}
