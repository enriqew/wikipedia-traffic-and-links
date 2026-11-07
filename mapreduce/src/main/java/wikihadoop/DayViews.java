package wikihadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/** One pageview_complete row's views, as ArticleDays shuffles them: day, agent, access method. */
public final class DayViews implements Writable {

  public static final String[] ACCESS = {"desktop", "mobile-web", "mobile-app"};

  public int day;
  public boolean automated;
  public byte access;
  public long views;

  public static byte access(String name) {
    for (byte i = 0; i < ACCESS.length; i++) {
      if (ACCESS[i].equals(name)) {
        return i;
      }
    }
    throw new IllegalArgumentException("unknown access method " + name);
  }

  public DayViews set(int day, boolean automated, byte access, long views) {
    this.day = day;
    this.automated = automated;
    this.access = access;
    this.views = views;
    return this;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(day);
    out.writeBoolean(automated);
    out.writeByte(access);
    out.writeLong(views);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    day = in.readInt();
    automated = in.readBoolean();
    access = in.readByte();
    views = in.readLong();
  }
}
