package ua.fim.eclat.util;

import static ua.fim.disteclat.util.TriePrinter.CLOSESUP;
import static ua.fim.disteclat.util.TriePrinter.OPENSUP;
import static ua.fim.disteclat.util.TriePrinter.SEPARATOR;
import static ua.fim.disteclat.util.TriePrinter.SYMBOL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import ua.hadoop.util.IntArrayWritable;

public interface SetReporter {
  public void report(int[] itemset, int support);
  
  public void close();
  
  static class Util {
    public static String getItemString(Collection<Item> itemset) {
      return getItemString(itemset, ' ');
    }
    
    public static String getItemString(Collection<Item> itemset, char separator) {
      StringBuilder builder = new StringBuilder();
      for (Item item : itemset) {
        builder.append(item.id + separator);
      }
      return builder.substring(0, builder.length() - 1);
    }
    
    public static String getSortedItemString(Collection<Item> itemset) {
      return getSortedItemString(itemset, ' ');
    }
    
    public static String getSortedItemString(Collection<Item> itemset, char separator) {
      List<String> sortedItems = new ArrayList<String>(itemset.size());
      for (Item item : itemset) {
        sortedItems.add(item.id + "");
      }
      Collections.sort(sortedItems);
      
      StringBuilder builder = new StringBuilder();
      for (String item : sortedItems) {
        builder.append(item + separator);
      }
      return builder.substring(0, builder.length() - 1);
    }
    
    public static String getIntsString(int[] ints) {
      if (ints.length == 0) {
        return "";
      }
      
      StringBuilder builder = new StringBuilder();
      for (int i : ints) {
        builder.append(i + " ");
      }
      return builder.substring(0, builder.length() - 1);
    }
    
    public static IntArrayWritable getIntArrayWritable(int[] ints) {
      IntWritable[] iw = new IntWritable[ints.length];
      for (int i = 0; i < ints.length; i++) {
        iw[i] = new IntWritable(ints[i]);
      }
      return new IntArrayWritable(iw);
    }
    
  }
  
  public static class HadoopPerLevelCountReporter implements SetReporter {
    
    private final Context context;
    Map<Integer,AtomicLong> counts = new HashMap<Integer,AtomicLong>();
    
    public HadoopPerLevelCountReporter(Context context) {
      this.context = context;
    }
    
    int count = 0;
    
    @Override
    public void report(int[] itemset, int support) {
      int size = itemset.length;
      AtomicLong count = counts.get(size);
      if (count == null) {
        count = new AtomicLong();
        counts.put(size, count);
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
  
  public static class CmdReporter implements SetReporter {
    
    @Override
    public void report(int[] itemset, int support) {
      StringBuilder builder = new StringBuilder();
      for (int item : itemset) {
        builder.append(item + " ");
      }
      builder.append("(" + support + ")");
      System.out.println(builder.toString());
    }
    
    @Override
    public void close() {}
    
  }
  
  public class HadoopTreeStringReporter implements SetReporter {
    private static final int MAX_SETS_BUFFER = 1000000;
    
    private final Context context;
    private final StringBuilder builder;
    
    private int[] prevSet;
    private int count;
    
    public HadoopTreeStringReporter(Context context) {
      this.context = context;
      builder = new StringBuilder();
      count = 0;
    }
    
    @Override
    public void report(int[] itemset, int support) {
      if (prevSet == null) {
        for (int i = 0; i < itemset.length - 1; i++) {
          builder.append(itemset[i]).append(SEPARATOR);
        }
      } else {
        int depth = 0;
        while (depth < itemset.length && depth < prevSet.length && itemset[depth] == prevSet[depth]) {
          depth++;
        }
        
        for (int i = prevSet.length - depth; i > 0; i--) {
          builder.append(SYMBOL);
        }
        for (int i = depth; i < itemset.length - 1; i++) {
          builder.append(itemset[i]).append(SEPARATOR);
        }
      }
      builder.append(itemset[itemset.length - 1]).append(OPENSUP).append(support).append(CLOSESUP);
      prevSet = Arrays.copyOf(itemset, itemset.length);
      count++;
      if (count % MAX_SETS_BUFFER == 0) {
        try {
          context.write(new Text("" + count), new Text(builder.toString()));
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        System.out.println("wrote " + count + " compressed itemsets");
        builder.setLength(0);
        count = 0;
      }
    }
    
    @Override
    public void close() {
      try {
        context.write(new Text("" + count), new Text(builder.toString()));
        System.out.println("wrote " + count + " compressed itemsets");
        builder.setLength(0);
        count = 0;
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
  }
}