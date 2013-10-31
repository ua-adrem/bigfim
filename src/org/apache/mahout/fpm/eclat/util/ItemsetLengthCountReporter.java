package org.apache.mahout.fpm.eclat.util;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ItemsetLengthCountReporter implements SetReporter {
  
  private final Context context;
  ConcurrentMap<Integer,AtomicLong> counts = new ConcurrentHashMap<Integer,AtomicLong>();
  
  public ItemsetLengthCountReporter(Context context) {
    this.context = context;
  }
  
  @Override
  public void report(int[] itemset, int support) {
    int size = itemset.length;
    AtomicLong count = counts.putIfAbsent(size, new AtomicLong());
    if (count == null) {
      count = counts.get(size);
    }
    count.incrementAndGet();
  }
  
  @Override
  public void close() {
    try {
      for (Entry<Integer,AtomicLong> entry : counts.entrySet()) {
        context.write(new Text("" + entry.getKey()), new LongWritable(entry.getValue().get()));
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  public long getCountForLevel(int level) {
    AtomicLong count = counts.get(level);
    if (count == null) {
      return 0;
    }
    return count.get();
  }
  
  public Set<Integer> getLevels() {
    return counts.keySet();
  }
}