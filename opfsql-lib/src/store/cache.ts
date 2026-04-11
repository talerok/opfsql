/** Read-through cache interface. Implementations can be swapped (LRU, clock, ARC, etc.). */
export interface ICache<K, V> {
  get(key: K): V | undefined;
  set(key: K, value: V): void;
  delete(key: K): void;
  has(key: K): boolean;
  clear(): void;
}

/**
 * Simple LRU cache using Map insertion order.
 * Map.delete + Map.set moves a key to the end (most recent).
 * Evicts the oldest entry (first key) when capacity is exceeded.
 */
export class LRUCache<K, V> implements ICache<K, V> {
  private readonly map = new Map<K, V>();

  constructor(private readonly capacity: number) {
    if (capacity <= 0) {
      throw new Error("Capacity must be > 0");
    }
  }

  get(key: K): V | undefined {
    if (!this.map.has(key)) {
      return undefined;
    }

    const val = this.map.get(key)!;
    this.map.delete(key);
    this.map.set(key, val);
    return val;
  }

  set(key: K, value: V): void {
    if (this.map.has(key)) {
      this.map.delete(key);
    }

    this.map.set(key, value);

    if (this.map.size > this.capacity) {
      this.removeFirst();
    }
  }

  delete(key: K): void {
    this.map.delete(key);
  }

  has(key: K): boolean {
    return this.map.has(key);
  }

  clear(): void {
    this.map.clear();
  }

  private removeFirst() {
    const key = this.map.keys().next().value!;
    this.map.delete(key);
  }
}
