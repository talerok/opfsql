import { bytesArgument, returns, strict } from "./arguments.js";
import type { ScalarFunction } from "./types.js";

// ---------------------------------------------------------------------------
// MD5 (RFC 1321) of a value's bytes: a BLOB's own, else the UTF-8 of its
// text — canonical JSON text for a JSON object or array.
// ---------------------------------------------------------------------------

/** One of MD5's four rounds of 16 steps. */
interface Round {
  /** The round's function of the state words b, c and d. */
  readonly mix: (b: number, c: number, d: number) => number;
  /** Which word of the block step `step` (0–63) adds. */
  readonly wordIndex: (step: number) => number;
  /** The left rotations of the round's steps, repeating every four. */
  readonly shifts: readonly [number, number, number, number];
}

const ROUNDS: readonly [Round, Round, Round, Round] = [
  {
    mix: (b, c, d) => (b & c) | (~b & d),
    wordIndex: (step) => step,
    shifts: [7, 12, 17, 22],
  },
  {
    mix: (b, c, d) => (d & b) | (~d & c),
    wordIndex: (step) => (5 * step + 1) % 16,
    shifts: [5, 9, 14, 20],
  },
  {
    mix: (b, c, d) => b ^ c ^ d,
    wordIndex: (step) => (3 * step + 5) % 16,
    shifts: [4, 11, 16, 23],
  },
  {
    mix: (b, c, d) => c ^ (b | ~d),
    wordIndex: (step) => (7 * step) % 16,
    shifts: [6, 10, 15, 21],
  },
];

/** floor(2^32 × |sin(i + 1)|) for i in 0..63. */
const SINES: readonly number[] = [
  0xd7_6a_a4_78, 0xe8_c7_b7_56, 0x24_20_70_db, 0xc1_bd_ce_ee, 0xf5_7c_0f_af,
  0x47_87_c6_2a, 0xa8_30_46_13, 0xfd_46_95_01, 0x69_80_98_d8, 0x8b_44_f7_af,
  0xff_ff_5b_b1, 0x89_5c_d7_be, 0x6b_90_11_22, 0xfd_98_71_93, 0xa6_79_43_8e,
  0x49_b4_08_21, 0xf6_1e_25_62, 0xc0_40_b3_40, 0x26_5e_5a_51, 0xe9_b6_c7_aa,
  0xd6_2f_10_5d, 0x02_44_14_53, 0xd8_a1_e6_81, 0xe7_d3_fb_c8, 0x21_e1_cd_e6,
  0xc3_37_07_d6, 0xf4_d5_0d_87, 0x45_5a_14_ed, 0xa9_e3_e9_05, 0xfc_ef_a3_f8,
  0x67_6f_02_d9, 0x8d_2a_4c_8a, 0xff_fa_39_42, 0x87_71_f6_81, 0x6d_9d_61_22,
  0xfd_e5_38_0c, 0xa4_be_ea_44, 0x4b_de_cf_a9, 0xf6_bb_4b_60, 0xbe_bf_bc_70,
  0x28_9b_7e_c6, 0xea_a1_27_fa, 0xd4_ef_30_85, 0x04_88_1d_05, 0xd9_d4_d0_39,
  0xe6_db_99_e5, 0x1f_a2_7c_f8, 0xc4_ac_56_65, 0xf4_29_22_44, 0x43_2a_ff_97,
  0xab_94_23_a7, 0xfc_93_a0_39, 0x65_5b_59_c3, 0x8f_0c_cc_92, 0xff_ef_f4_7d,
  0x85_84_5d_d1, 0x6f_a8_7e_4f, 0xfe_2c_e6_e0, 0xa3_01_43_14, 0x4e_08_11_a1,
  0xf7_53_7e_82, 0xbd_3a_f2_35, 0x2a_d7_d2_bb, 0xeb_86_d3_91,
];

/** The MD5 digest of `bytes` as 32 lowercase hex digits. */
export function md5(bytes: Uint8Array): string {
  const words = padded(bytes);
  let state: State = [
    0x67_45_23_01, 0xef_cd_ab_89, 0x98_ba_dc_fe, 0x10_32_54_76,
  ];
  for (let offset = 0; offset < words.length; offset += 16) {
    state = compress(state, words, offset);
  }
  return state.map((word) => littleEndianHex(word)).join("");
}

/** The four 32-bit words of MD5's state, as unsigned numbers. */
type State = readonly [number, number, number, number];

/** The message padded to 64-byte blocks, as little-endian 32-bit words. */
function padded(bytes: Uint8Array): Uint32Array {
  const blocks = Math.floor((bytes.length + 8) / 64) + 1;
  const buffer = new Uint8Array(blocks * 64);
  buffer.set(bytes);
  buffer[bytes.length] = 0x80;
  const view = new DataView(buffer.buffer);
  const bits = bytes.length * 8;
  view.setUint32(buffer.length - 8, bits % 2 ** 32, true);
  view.setUint32(buffer.length - 4, Math.floor(bits / 2 ** 32), true);
  const words = new Uint32Array(blocks * 16);
  for (let index = 0; index < words.length; index++) {
    words[index] = view.getUint32(index * 4, true);
  }
  return words;
}

function compress(state: State, words: Uint32Array, offset: number): State {
  let [a, b, c, d] = state;
  for (let step = 0; step < 64; step++) {
    const round = ROUNDS[Math.floor(step / 16) as 0 | 1 | 2 | 3];
    const word = words[offset + round.wordIndex(step)] ?? 0;
    const shift = round.shifts[(step % 4) as 0 | 1 | 2 | 3];
    const mixed = add32(round.mix(b, c, d), a, SINES[step] ?? 0, word);
    a = d;
    d = c;
    c = b;
    b = add32(b, rotateLeft(mixed, shift));
  }
  return [
    add32(state[0], a),
    add32(state[1], b),
    add32(state[2], c),
    add32(state[3], d),
  ];
}

/** The sum of 32-bit words, modulo 2^32. */
function add32(...words: number[]): number {
  return words.reduce((sum, word) => sum + word, 0) >>> 0;
}

function rotateLeft(value: number, shift: number): number {
  return (value << shift) | (value >>> (32 - shift));
}

function littleEndianHex(word: number): string {
  let hex = "";
  for (let byte = 0; byte < 4; byte++) {
    hex += ((word >>> (byte * 8)) & 0xff).toString(16).padStart(2, "0");
  }
  return hex;
}

const md5Function: ScalarFunction = {
  kind: "scalar",
  name: "md5",
  signature: { required: ["any"] },
  returnType: returns("TEXT"),
  implement: () => strict(([value = null]) => md5(bytesArgument(value))),
};

export const hashFunctions: readonly ScalarFunction[] = [md5Function];
