package ua.fim.disteclat.util;

import static java.lang.Integer.parseInt;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static class PrefixItemTIDsReporter implements SetReporter {
    
    private Context context;
    private int prefixLength;
    
    public PrefixItemTIDsReporter(Context context, int prefixLength) {
      this.context = context;
      this.prefixLength = prefixLength;
    }
    
    @Override
    public void report(List<Item> itemset, int support) {
      throw new UnsupportedOperationException("This reporter needs TIDs!");
    }
    
    @Override
    public void report(List<Item> itemset, int support, int[] tids) {
      StringBuilder sb = new StringBuilder();
      if(itemset.size() < prefixLength){
        System.out.println("Found a short fis:" + itemset);
        return;
      }
      int prefixStrLength = 0;
      Item lastItem = null;
      for (Item item : itemset) {
        prefixStrLength = sb.length() - 1;
        sb.append(item.name).append(" ");
        lastItem = item;
      }
      sb.setLength(prefixStrLength);
      
      Text key = new Text(sb.toString());
      
      IntWritable[] iw = new IntWritable[tids.length + 1];
      
      for (int i = 1; i < iw.length; i++) {
        iw[i]= new IntWritable(tids[i - 1]);
      }
      iw[0]= new IntWritable(parseInt(lastItem.name));
      
      try {
        context.write(key, new IntArrayWritable(iw));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    @Override
    public void close() {
      
    }
    
  }
}