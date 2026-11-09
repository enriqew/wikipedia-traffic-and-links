package wikihadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/** A title's views by agent: {@code user} and {@code automated}. */
public final class AgentViews implements Writable {

  public long user;
  public long automated;

  public void set(long user, long automated) {
    this.user = user;
    this.automated = automated;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(user);
    out.writeLong(automated);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    user = in.readLong();
    automated = in.readLong();
  }
}
