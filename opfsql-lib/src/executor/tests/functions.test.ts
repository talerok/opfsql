import { describe, it, expect } from 'vitest';
import { md5 } from '../evaluate/functions/md5.js';
import { normalizeJson } from '../evaluate/functions/normalize-json.js';

// ---------------------------------------------------------------------------
// md5
// ---------------------------------------------------------------------------

describe('md5', () => {
  it('empty string', () => {
    expect(md5('')).toBe('d41d8cd98f00b204e9800998ecf8427e');
  });

  it('simple string "abc"', () => {
    expect(md5('abc')).toBe('900150983cd24fb0d6963f7d28e17f72');
  });

  it('"hello world"', () => {
    expect(md5('hello world')).toBe('5eb63bbbe01eeed093cb22bb8f5acdc3');
  });

  it('numeric string', () => {
    expect(md5('123456')).toBe('e10adc3949ba59abbe56e057f20f883e');
  });

  it('UTF-8 characters', () => {
    // MD5 of "Привет" (Russian for "Hello")
    const hash = md5('Привет');
    expect(hash).toHaveLength(32);
    expect(/^[0-9a-f]{32}$/.test(hash)).toBe(true);
  });

  it('long string', () => {
    const long = 'a'.repeat(1000);
    const hash = md5(long);
    expect(hash).toHaveLength(32);
    expect(/^[0-9a-f]{32}$/.test(hash)).toBe(true);
  });

  it('different inputs produce different hashes', () => {
    expect(md5('a')).not.toBe(md5('b'));
  });

  it('same input produces same hash', () => {
    expect(md5('test')).toBe(md5('test'));
  });
});

// ---------------------------------------------------------------------------
// normalizeJson
// ---------------------------------------------------------------------------

describe('normalizeJson', () => {
  it('null passes through', () => {
    expect(normalizeJson(null)).toBeNull();
  });

  it('primitives pass through', () => {
    expect(normalizeJson(42)).toBe(42);
    expect(normalizeJson('hello')).toBe('hello');
    expect(normalizeJson(true)).toBe(true);
  });

  it('sorts object keys', () => {
    expect(normalizeJson({ b: 2, a: 1 })).toEqual({ a: 1, b: 2 });
  });

  it('preserves array order', () => {
    expect(normalizeJson([3, 1, 2])).toEqual([3, 1, 2]);
  });

  it('recursively normalizes nested objects', () => {
    const input = { z: { b: 2, a: 1 }, a: [{ y: 1, x: 2 }] };
    const expected = { a: [{ x: 2, y: 1 }], z: { a: 1, b: 2 } };
    expect(normalizeJson(input)).toEqual(expected);
  });

  it('handles deeply nested arrays', () => {
    const input = [[{ b: 1, a: 2 }], [3]];
    const expected = [[{ a: 2, b: 1 }], [3]];
    expect(normalizeJson(input)).toEqual(expected);
  });

  it('empty object stays empty', () => {
    expect(normalizeJson({})).toEqual({});
  });

  it('empty array stays empty', () => {
    expect(normalizeJson([])).toEqual([]);
  });
});
