package com.noahmern.arkdb.memtable;

public class MemtableManager {
  private static final int MEMTABLE_SIZE = 64 * 1024 * 1024; // 64 MB

  private final SkipList[] active;
  private final SkipList[] immutable;

  public MemtableManager(int maxMemtables) {
    // we should have at most 2 active memtables
    this.active = new SkipList[2];
    this.immutable = new SkipList[maxMemtables - 2];
  }
}
