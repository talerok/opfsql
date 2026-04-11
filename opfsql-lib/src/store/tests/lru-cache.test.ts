import { describe, it, expect } from 'vitest';
import { LRUCache } from '../cache.js';

describe('LRUCache', () => {
  it('get/set basic', () => {
    const c = new LRUCache<string, number>(3);
    c.set('a', 1);
    expect(c.get('a')).toBe(1);
    expect(c.get('b')).toBeUndefined();
  });

  it('evicts oldest when over capacity', () => {
    const c = new LRUCache<string, number>(2);
    c.set('a', 1);
    c.set('b', 2);
    c.set('c', 3); // evicts 'a'
    expect(c.get('a')).toBeUndefined();
    expect(c.get('b')).toBe(2);
    expect(c.get('c')).toBe(3);
  });

  it('get promotes entry (not evicted next)', () => {
    const c = new LRUCache<string, number>(2);
    c.set('a', 1);
    c.set('b', 2);
    c.get('a'); // promote 'a'
    c.set('c', 3); // evicts 'b' (oldest after promotion)
    expect(c.get('a')).toBe(1);
    expect(c.get('b')).toBeUndefined();
    expect(c.get('c')).toBe(3);
  });

  it('set overwrites and promotes', () => {
    const c = new LRUCache<string, number>(2);
    c.set('a', 1);
    c.set('b', 2);
    c.set('a', 10); // overwrite + promote
    c.set('c', 3); // evicts 'b'
    expect(c.get('a')).toBe(10);
    expect(c.get('b')).toBeUndefined();
  });

  it('delete removes entry', () => {
    const c = new LRUCache<string, number>(3);
    c.set('a', 1);
    c.delete('a');
    expect(c.has('a')).toBe(false);
    expect(c.get('a')).toBeUndefined();
  });

  it('clear removes all', () => {
    const c = new LRUCache<string, number>(3);
    c.set('a', 1);
    c.set('b', 2);
    c.clear();
    expect(c.has('a')).toBe(false);
    expect(c.has('b')).toBe(false);
  });
});
