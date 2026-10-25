// CRC-32 (IEEE), computed over several parts as if they were one.

const TABLE = Uint32Array.from({ length: 256 }, (_, byte) => {
  let crc = byte;
  for (let bit = 0; bit < 8; bit++) {
    crc = (crc & 1) === 1 ? 0xed_b8_83_20 ^ (crc >>> 1) : crc >>> 1;
  }
  return crc;
});

export function crc32(...parts: readonly Uint8Array[]): number {
  let crc = 0xff_ff_ff_ff;
  for (const part of parts) {
    for (const byte of part) {
      crc = (TABLE[(crc ^ byte) & 0xff] ?? 0) ^ (crc >>> 8);
    }
  }
  return (crc ^ 0xff_ff_ff_ff) >>> 0;
}
