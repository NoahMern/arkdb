package com.noahmern.arkdb.memtable;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_SHORT;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class SkipList {
  private static final int MAX_HEIGHT = 20;

  /*
  Node layout:
  - value: 8 bytes (long)
  - keyOffset: 4 bytes (int)
  - keySize: 2 bytes (short)
  - height: 2 bytes (short)
  */
  private static final MemoryLayout NODE_HEADER_LAYOUT =
     MemoryLayout.structLayout(
      JAVA_LONG.withName("value"), // (valueOffset, valueSize)
      JAVA_INT.withName("keyOffset"),
      JAVA_SHORT.withName("keySize"),
      JAVA_SHORT.withName("height")
     );

  private static final VarHandle VALUE_HANDLE = NODE_HEADER_LAYOUT.varHandle(
    MemoryLayout.PathElement.groupElement("value"));
  private static final VarHandle KEY_OFFSET_HANDLE = NODE_HEADER_LAYOUT.varHandle(
    MemoryLayout.PathElement.groupElement("keyOffset"));
  private static final VarHandle KEY_SIZE_HANDLE = NODE_HEADER_LAYOUT.varHandle(
    MemoryLayout.PathElement.groupElement("keySize"));
  private static final VarHandle HEIGHT_HANDLE = NODE_HEADER_LAYOUT.varHandle(
    MemoryLayout.PathElement.groupElement("height"));
  private static final VarHandle NEXT_HANDLE = JAVA_INT.varHandle();

  private static final int NODE_SIZE = (int) NODE_HEADER_LAYOUT.byteSize();

  private static final int NULL = 0;

  public static final int OUT_OF_MEMORY = -1;

  private final MemorySegment memory;
  private final int memorySize;
  // memory 0 is reserved for representing NULL pointer
  private final AtomicInteger head = new AtomicInteger(1);
  private final int rootOffset;

  public SkipList(MemorySegment memory) {
    this.memory = memory;
    this.memorySize = (int) memory.byteSize();
    this.rootOffset = newNode(new byte[0], new byte[0], MAX_HEIGHT);
  }

  private int newNode(byte[] key, byte[] value,int height) {
    // <padding><node_header><next_offsets><key><value><padding>
    int headerOff = 0;
    int towerOff  = NODE_SIZE;
    int keyOff    = towerOff + height * Integer.BYTES;
    int valOff    = keyOff + key.length;

    int nodeSize = valOff + value.length;
    long offset = reserve(nodeSize);
    if (offset == OUT_OF_MEMORY) {
      return OUT_OF_MEMORY;
    }

    long packedValue = ((valOff & 0xffffffffL) << 32) | (value.length & 0xffffffffL);
    VALUE_HANDLE.set(memory, offset + headerOff, packedValue);
    KEY_OFFSET_HANDLE.set(memory, offset + keyOff, (int)(offset + keyOff));
    KEY_SIZE_HANDLE.set(memory, offset + keyOff, (short) key.length);
    HEIGHT_HANDLE.set(memory, offset + headerOff, (short) height);

    long towerBase = (long) offset + NODE_SIZE;
    for (int lvl = 0; lvl < height; lvl++) {
      NEXT_HANDLE.set(memory, towerBase + (long) lvl * Integer.BYTES, NULL);
    }

    MemorySegment.copy(key,   0, memory, JAVA_BYTE, offset + keyOff, key.length);
    MemorySegment.copy(value, 0, memory, JAVA_BYTE, offset + valOff, value.length);

    return (int) offset;
  }

  private int reserve(int bytes) {
    int cur, start, next;
    int total = align(bytes); // align allocation size
    do {
      cur = head.get();
      start = align(cur);
      next = start + total;
      if (next > memorySize) return OUT_OF_MEMORY;
    } while (!head.compareAndSet(cur, next));
    return start;
  }

  private int getRandomHeights() {
    int r = FastRand.nextInt();
    int zeros = Integer.numberOfTrailingZeros(r) >> 1;
    return Math.min(zeros + 1, MAX_HEIGHT);
  }

  public void reset() {
    head.set(1);
  }

  private static int align(int x) {
    return (x + 7) & ~7;
  }

}
