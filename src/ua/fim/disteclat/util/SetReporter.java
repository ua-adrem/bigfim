package ua.fim.disteclat.util;

import static ua.fim.disteclat.util.SetReporter.Util.getItemString;
import static ua.fim.disteclat.util.TriePrinter.CLOSESUP;
import static ua.fim.disteclat.util.TriePrinter.OPENSUP;
import static ua.fim.disteclat.util.TriePrinter.SEPARATOR;
import static ua.fim.disteclat.util.TriePrinter.SYMBOL;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

import ua.hadoop.util.IntArrayWritable;

public interface SetReporter {
  public void report(List<Item> itemset, int support);
  
  public void report(List<Item> itemset, int support, int[] ints);
  
  public void close();
  
  static class Util {
    public static String getItemString(List<Item> itemset) {
      return getItemString(itemset, ' ');
    }
    
    public static String getItemString(List<Item> itemset, char separator) {
      StringBuilder builder = new StringBuilder();
      for (Item item : itemset) {
        builder.append(item.name + separator);
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
  
  public static class NullReporter implements SetReporter {
    @Override
    public void report(List<Item> itemset, int support, int[] ints) {}
    
    @Override
    public void report(List<Item> itemset, int support) {}
    
    @Override
    public void close() {}
    
  }
  
  public static class CommandLineReporter implements SetReporter {
    
    @Override
    public void report(List<Item> itemset, int support, int[] ints) {
      report(itemset, support);
    }
    
    @Override
    public void report(List<Item> itemset, int support) {
      System.out.println(Util.getItemString(itemset) + " (" + support + ")");
    }
    
    @Override
    public void close() {}
    
  }
  
  public static class ToFileReporter implements SetReporter {
    public BufferedWriter writer;
    
    public ToFileReporter(String outputFile) {
      try {
        writer = new BufferedWriter(new FileWriter(outputFile));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    @Override
    public void report(List<Item> itemset, int support, int[] ints) {
      report(itemset, support);
    }
    
    @Override
    public void report(List<Item> itemset, int support) {
      try {
        writer.write(Util.getItemString(itemset) + " (" + support + ")\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    @Override
    public void close() {
      try {
        writer.flush();
        writer.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  public static class HadoopPerLevelCountReporter implements SetReporter {
    
    private final Context context;
    Map<Integer,AtomicLong> counts = new HashMap<Integer,AtomicLong>();
    
    public HadoopPerLevelCountReporter(Context context) {
      this.context = context;
    }
    
    @Override
    public void report(List<Item> itemset, int support, int[] ints) {
      report(itemset, support);
    }
    
    @Override
    public void report(List<Item> itemset, int support) {
      int size = itemset.size();
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
    
    public int getMaxLevel() {
      int maxLevel = -1;
      for (int level : counts.keySet()) {
        maxLevel = Math.max(level, maxLevel);
      }
      return maxLevel;
    }
  }
  
  public static class HadoopPerLevelReporter implements SetReporter {
    
    private final Map<Integer,List<String>> freqSetsPerLevel = new HashMap<Integer,List<String>>();
    private final Context context;
    private final String fisKey;
    private final String prefixKey;
    private final int prefixLength;
    private String delimiter;
    
    public HadoopPerLevelReporter(Context context, String fisKey, String prefixKey, int prefixLength) {
      this.context = context;
      this.fisKey = fisKey;
      this.prefixKey = prefixKey;
      this.prefixLength = prefixLength;
    }
    
    public HadoopPerLevelReporter(Context context, String fis, String prefix, int prefixLength, String delimiter) {
      this(context, fis, prefix, prefixLength);
      this.delimiter = delimiter;
    }
    
    @Override
    public void report(List<Item> itemset, int support, int[] ints) {
      report(itemset, support);
      if (itemset.size() != prefixLength) {
        return;
      }
      try {
        String itemString = getItemString(itemset);
        context.write(new Text(prefixKey + delimiter + itemString + delimiter + support), new ObjectWritable(ints));
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    @Override
    public void report(List<Item> itemset, int support) {
      List<String> freqSetsOfLevel = freqSetsPerLevel.get(itemset.size());
      if (freqSetsOfLevel == null) {
        freqSetsOfLevel = new LinkedList<String>();
        freqSetsPerLevel.put(itemset.size(), freqSetsOfLevel);
      }
      freqSetsOfLevel.add(Util.getItemString(itemset) + " (" + support + ")");
    }
    
    @Override
    public void close() {
      for (Entry<Integer,List<String>> entry : freqSetsPerLevel.entrySet()) {
        StringBuilder builder = new StringBuilder();
        for (String set : entry.getValue()) {
          builder.append(set + " ");
        }
        try {
          context.write(new Text(fisKey), new ObjectWritable(builder.toString()));
        } catch (IOException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    
    public List<String> getFreqSetsOfLevel(int level) {
      return freqSetsPerLevel.get(level);
    }
  }
  
  public class HadoopTreeStringReporter implements SetReporter {
    private static final int MAX_SETS_BUFFER = 1000000;
    
    private final Context context;
    private final StringBuilder builder;
    
    private List<Item> prevSet;
    private int count;
    
    public HadoopTreeStringReporter(Context context) {
      this.context = context;
      builder = new StringBuilder();
      count = 0;
    }
    
    @Override
    public void report(List<Item> itemset, int support, int[] ints) {
      report(itemset, support);
    }
    
    @Override
    public void report(List<Item> itemset, int support) {
      if (prevSet == null) {
        for (int i = 0; i < itemset.size() - 1; i++) {
          builder.append(itemset.get(i).name + SEPARATOR);
        }
      } else {
        int depth = 0;
        while (depth < itemset.size() && depth < prevSet.size()
            && itemset.get(depth).name.equals(prevSet.get(depth).name)) {
          depth++;
        }
        
        for (int i = prevSet.size() - depth; i > 0; i--) {
          builder.append(SYMBOL);
        }
        for (int i = depth; i < itemset.size() - 1; i++) {
          builder.append(itemset.get(i).name + SEPARATOR);
        }
      }
      builder.append(itemset.get(itemset.size() - 1).name + OPENSUP + support + CLOSESUP);
      prevSet = new ArrayList<Item>(itemset);
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
  
  public class TreeStringReporter implements SetReporter {
    
    private static final int MAX_SETS_BUFFER = 1000000;
    
    private final PrintStream p;
    private final StringBuilder builder;
    
    private int count = 0;
    private List<Item> prevSet = null;
    
    public TreeStringReporter(PrintStream p) {
      this.p = p;
      builder = new StringBuilder();
    }
    
    public TreeStringReporter() {
      this(new PrintStream(System.out));
    }
    
    @Override
    public void report(List<Item> itemset, int support, int[] ints) {
      report(itemset, support);
    }
    
    @Override
    public void report(List<Item> itemset, int support) {
      if (prevSet == null) {
        for (int i = 0; i < itemset.size() - 1; i++) {
          builder.append(itemset.get(i).name + SEPARATOR);
        }
      } else {
        int depth = 0;
        while (depth < itemset.size() && depth < prevSet.size()
            && itemset.get(depth).name.equals(prevSet.get(depth).name)) {
          depth++;
        }
        
        for (int i = prevSet.size() - depth; i > 0; i--) {
          builder.append(SYMBOL);
        }
        for (int i = depth; i < itemset.size() - 1; i++) {
          builder.append(itemset.get(i).name + SEPARATOR);
        }
      }
      builder.append(itemset.get(itemset.size() - 1).name + OPENSUP + support + CLOSESUP);
      prevSet = new ArrayList<Item>(itemset);
      count++;
      if (count % MAX_SETS_BUFFER == 0) {
        p.println(builder.toString());
        System.out.println("wrote " + count + " compressed itemsets");
        builder.setLength(0);
        count = 0;
      }
    }
    
    @Override
    public void close() {
      if (builder.length() != 0) {
        p.println(builder.toString());
      }
    }
  }
}