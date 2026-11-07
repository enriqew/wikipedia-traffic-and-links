package wikihadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * The composite key of ArticleDays: an article and a day of the window.
 *
 * <p>The secondary sort: keys sort by wiki, title and day, but {@link ByArticle} partitions and
 * {@link GroupByArticle} groups on wiki and title alone, so one reduce call receives all of an
 * article's days, in day order, without the reducer sorting anything.
 */
public final class ArticleDayKey implements WritableComparable<ArticleDayKey> {

  public final Text wiki = new Text();
  public final Text title = new Text();
  public int day;

  public ArticleDayKey set(String wiki, String title, int day) {
    this.wiki.set(wiki);
    this.title.set(title);
    this.day = day;
    return this;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    wiki.write(out);
    title.write(out);
    out.writeInt(day);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    wiki.readFields(in);
    title.readFields(in);
    day = in.readInt();
  }

  int compareArticle(ArticleDayKey o) {
    int c = wiki.compareTo(o.wiki);
    return c != 0 ? c : title.compareTo(o.title);
  }

  @Override
  public int compareTo(ArticleDayKey o) {
    int c = compareArticle(o);
    return c != 0 ? c : Integer.compare(day, o.day);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof ArticleDayKey && compareTo((ArticleDayKey) o) == 0;
  }

  @Override
  public int hashCode() {
    return (wiki.hashCode() * 31 + title.hashCode()) * 31 + day;
  }

  /** Sends all of an article's days to the same reducer. */
  public static final class ByArticle extends Partitioner<ArticleDayKey, DayViews> {
    @Override
    public int getPartition(ArticleDayKey key, DayViews value, int partitions) {
      return ((key.wiki.hashCode() * 31 + key.title.hashCode()) & Integer.MAX_VALUE) % partitions;
    }
  }

  /** Makes all of an article's days one reduce call. */
  public static final class GroupByArticle extends WritableComparator {
    public GroupByArticle() {
      super(ArticleDayKey.class, true);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
      return ((ArticleDayKey) a).compareArticle((ArticleDayKey) b);
    }
  }
}
