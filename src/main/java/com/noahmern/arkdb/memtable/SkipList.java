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
  - tower: MAX_LEVEL * 4 bytes (int array of offsets to next nodes)
  */
  private static final MemoryLayout NODE_LAYOUT =
     MemoryLayout.structLayout(
      JAVA_LONG.withName("value"), // (valueOffset, valueSize)
      JAVA_INT.withName("keyOffset"),
      JAVA_SHORT.withName("keySize"),
      JAVA_SHORT.withName("height")
     );

  private static final VarHandle VALUE_HANDLE = NODE_LAYOUT.varHandle(
    MemoryLayout.PathElement.groupElement("value"));
  private static final VarHandle KEY_OFFSET_HANDLE = NODE_LAYOUT.varHandle(
    MemoryLayout.PathElement.groupElement("keyOffset"));
  private static final VarHandle KEY_SIZE_HANDLE = NODE_LAYOUT.varHandle(
    MemoryLayout.PathElement.groupElement("keySize"));
  private static final VarHandle HEIGHT_HANDLE = NODE_LAYOUT.varHandle(
    MemoryLayout.PathElement.groupElement("height"));

  private static final int NODE_SIZE = (int) NODE_LAYOUT.byteSize();

  private static final int NULL = 0;

  public static final int OUT_OF_MEMORY = -1;

  private final MemorySegment memory;
  private final int memorySize;
  // memory 0 is reserved for representing NULL pointer

  private final AtomicInteger head = new AtomicInteger(1);
  private final AtomicInteger height = new AtomicInteger(1);

  // offsets of head's next nodes at each level
  private final int[] nextOffsets = new int[MAX_HEIGHT];

  public SkipList(MemorySegment memory) {
    this.memory = memory;
    this.memorySize = (int) memory.byteSize();
  }

  private int newNode(byte[] key, byte[] value,int height) {
    int nodeOffset = allocateNode(height);
    if (nodeOffset == OUT_OF_MEMORY) {
      return OUT_OF_MEMORY;
    }
    int valueOffset = putBytes(value);
    if (valueOffset == OUT_OF_MEMORY) {
      return OUT_OF_MEMORY;
    }
    int keyOffset = putBytes(key);
    if (keyOffset == OUT_OF_MEMORY) {
      return OUT_OF_MEMORY;
    }
    long packedValue = ((long) valueOffset << 32) | value.length;
    VALUE_HANDLE.set(memory, nodeOffset, packedValue);
    KEY_OFFSET_HANDLE.set(memory, nodeOffset, keyOffset);
    KEY_SIZE_HANDLE.set(memory, nodeOffset, (short) key.length);
    HEIGHT_HANDLE.set(memory, nodeOffset, (short) height);
    // do we need to int the tower with NULL?
    return nodeOffset;
  }

  private int allocateNode(int height) {
    int nodeSize = NODE_SIZE + height * Integer.BYTES;

    int cur, start, next;
    do {
      cur = head.get();
      start = align(cur, 8);
      next  = start + nodeSize;
      if (next > memorySize) return OUT_OF_MEMORY;
    } while (!head.compareAndSet(cur, next));

    return start;
  }

  private static int align(int x, int alignment) {
    return (x + 7) & ~7;
  }

  private int putBytes(byte[] bytes) {
    int offset = head.getAndAdd(bytes.length);
    if (offset + bytes.length > memorySize) {
      return OUT_OF_MEMORY;
    }
    MemorySegment.copy(bytes, 0, this.memory, JAVA_BYTE, offset, bytes.length);
    return offset;
  }

  private int getRandomHights() {
    int r = FastRand.nextInt();
    int zeros = Integer.numberOfTrailingZeros(r) >> 1;
    return Math.min(zeros + 1, MAX_HEIGHT);
  }

  public void reset() {
    head.set(1);
    height.set(1);
  }

}
