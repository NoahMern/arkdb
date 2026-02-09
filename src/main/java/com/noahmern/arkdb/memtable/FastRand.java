package com.noahmern.arkdb.memtable;

// https://en.wikipedia.org/wiki/Xorshift
public class FastRand {
  private static final ThreadLocal<Long> STATE =
      ThreadLocal.withInitial(() -> seed());

  private static long seed() {
    long x = System.nanoTime();
    x ^= x << 21;
    x ^= x >>> 35;
    x ^= x << 4;
    return x | 1;
  }

  public static int nextInt() {
    long x = STATE.get();
    x ^= x << 13;
    x ^= x >>> 7;
    x ^= x << 17;
    STATE.set(x);
    return (int)x;
  }
}
