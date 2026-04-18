// ---------------------------------------------------------------------------
// Pure JS sync MD5 implementation (RFC 1321)
// No dependencies. Returns 32-char lowercase hex string.
// ---------------------------------------------------------------------------

export function md5(input: string): string {
  const bytes = encodeUTF8(input);
  const padded = pad(bytes);
  const view = new DataView(padded.buffer, padded.byteOffset, padded.byteLength);

  let a0 = 0x67452301;
  let b0 = 0xefcdab89;
  let c0 = 0x98badcfe;
  let d0 = 0x10325476;

  for (let offset = 0; offset < padded.length; offset += 64) {
    let a = a0, b = b0, c = c0, d = d0;

    for (let i = 0; i < 64; i++) {
      let f: number, g: number;
      if (i < 16) {
        f = (b & c) | (~b & d);
        g = i;
      } else if (i < 32) {
        f = (d & b) | (~d & c);
        g = (5 * i + 1) % 16;
      } else if (i < 48) {
        f = b ^ c ^ d;
        g = (3 * i + 5) % 16;
      } else {
        f = c ^ (b | ~d);
        g = (7 * i) % 16;
      }

      const m = view.getInt32(offset + g * 4, true);
      f = (f + a + K[i] + m) | 0;
      a = d;
      d = c;
      c = b;
      b = (b + rotl(f, S[i])) | 0;
    }

    a0 = (a0 + a) | 0;
    b0 = (b0 + b) | 0;
    c0 = (c0 + c) | 0;
    d0 = (d0 + d) | 0;
  }

  return toHex(a0) + toHex(b0) + toHex(c0) + toHex(d0);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function encodeUTF8(s: string): Uint8Array {
  const encoder = new TextEncoder();
  return encoder.encode(s);
}

function pad(bytes: Uint8Array): Uint8Array {
  const bitLen = bytes.length * 8;
  // Need: original + 1 byte (0x80) + padding + 8 bytes length
  // Total must be multiple of 64
  let totalLen = bytes.length + 1;
  while (totalLen % 64 !== 56) totalLen++;
  totalLen += 8;

  const padded = new Uint8Array(totalLen);
  padded.set(bytes);
  padded[bytes.length] = 0x80;

  // Append original length in bits as 64-bit little-endian
  const view = new DataView(padded.buffer);
  view.setUint32(totalLen - 8, bitLen >>> 0, true);
  view.setUint32(totalLen - 4, Math.floor(bitLen / 0x100000000), true);

  return padded;
}

function rotl(x: number, n: number): number {
  return (x << n) | (x >>> (32 - n));
}

function toHex(n: number): string {
  return (n >>> 0).toString(16).padStart(8, '0')
    .replace(/(..)(..)(..)(..)/, (_, a, b, c, d) => d + c + b + a);
}

// Per-round shift amounts
const S: number[] = [
  7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
  5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
  4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
  6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
];

// Pre-computed constants: floor(2^32 * abs(sin(i+1))) for i in 0..63
const K: number[] = [
  0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
  0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
  0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
  0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
  0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
  0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
  0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
  0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
  0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
  0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
  0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05,
  0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
  0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039,
  0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
  0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
  0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
];
