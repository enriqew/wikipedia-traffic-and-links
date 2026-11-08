package wikihadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * What PageRank's jobs pass between them for one page id: either the article's record, which
 * carries the graph from one iteration to the next, or a contribution of mass along a link.
 *
 * <p>A record holds the mass its article received in the iteration that wrote it ({@link #rank},
 * before the pool's share is added), the full rank the previous iteration used ({@link #previous})
 * and the out-links, sorted ascending. On disk the out-links are variable-length deltas, so a
 * record costs about one byte per link instead of a decimal id.
 */
public final class NodeWritable implements Writable {

  private boolean record;
  private long rank;
  private long previous;
  private long[] targets = new long[0];
  private int size;

  public boolean isRecord() {
    return record;
  }

  public long rank() {
    return rank;
  }

  public long previous() {
    return previous;
  }

  public int size() {
    return size;
  }

  public long target(int i) {
    return targets[i];
  }

  public NodeWritable contribution(long mass) {
    record = false;
    rank = mass;
    previous = 0;
    size = 0;
    return this;
  }

  /** An article's record; {@code links} must be sorted ascending without repeats. */
  public NodeWritable record(long rank, long previous, long[] links, int count) {
    for (int i = 1; i < count; i++) {
      if (links[i] <= links[i - 1]) {
        throw new IllegalArgumentException("out-links not strictly ascending at " + i);
      }
    }
    record = true;
    this.rank = rank;
    this.previous = previous;
    if (targets.length < count) {
      targets = new long[count];
    }
    System.arraycopy(links, 0, targets, 0, count);
    size = count;
    return this;
  }

  /** The same record with a new rank and previous rank, its out-links kept. */
  public NodeWritable rerank(long rank, long previous) {
    this.rank = rank;
    this.previous = previous;
    return this;
  }

  /** Copies {@code other} into this one: Hadoop reuses the value object a reducer iterates with. */
  public NodeWritable set(NodeWritable other) {
    record = other.record;
    rank = other.rank;
    previous = other.previous;
    if (targets.length < other.size) {
      targets = new long[other.size];
    }
    System.arraycopy(other.targets, 0, targets, 0, other.size);
    size = other.size;
    return this;
  }

  public long[] copyOfTargets() {
    return Arrays.copyOf(targets, size);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(record);
    WritableUtils.writeVLong(out, rank);
    if (record) {
      WritableUtils.writeVLong(out, previous);
      WritableUtils.writeVInt(out, size);
      long last = 0;
      for (int i = 0; i < size; i++) {
        WritableUtils.writeVLong(out, targets[i] - last);
        last = targets[i];
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    record = in.readBoolean();
    rank = WritableUtils.readVLong(in);
    if (record) {
      previous = WritableUtils.readVLong(in);
      size = WritableUtils.readVInt(in);
      if (targets.length < size) {
        targets = new long[size];
      }
      long last = 0;
      for (int i = 0; i < size; i++) {
        last += WritableUtils.readVLong(in);
        targets[i] = last;
      }
    } else {
      previous = 0;
      size = 0;
    }
  }
}
