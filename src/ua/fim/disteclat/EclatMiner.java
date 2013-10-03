package ua.fim.disteclat;

import static ua.fim.disteclat.util.Utils.setDifference;
import static ua.util.Tools.intersect;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import ua.fim.disteclat.util.Item;
import ua.fim.disteclat.util.SetReporter;
import ua.fim.disteclat.util.Utils;
import ua.hadoop.util.IntArrayWritable;

public class EclatMiner {
  
  public static enum Version {
    Eclat, DEclat
  }
  
  public static boolean closureCheck(Item newItem) {
    return newItem.getTids().length == 0;
  }
  
  private SetReporter reporter = new SetReporter.NullReporter();
  
  private int minSize = 0;
  private int maxSize = Integer.MAX_VALUE;
  
  private long setupCost = 0;
  private long recursiveCost = 0;
  private Map<String,Integer> indices;
  private List<Item> singletons;
  
  public long getSetupCost() {
    return setupCost;
  }
  
  public long getRecursiveCost() {
    return recursiveCost;
  }
  
  public void setMinSize(int minSize) {
    this.minSize = minSize;
  }
  
  public void setMaxSize(int maxSize) {
    this.maxSize = maxSize;
  }
  
  public void setSetReporter(SetReporter reporter) {
    this.reporter = reporter;
  }
  
  public int mine(Map<String,Integer> indices, List<Item> singletons, String prefix, int minSup) {
    this.indices = indices;
    this.singletons = singletons;
    
    long beg = System.currentTimeMillis();
    String[] itemset = prefix.split("_");
    
    Item item = singletons.get(indices.get(itemset[0]));
    int[] tids = item.getTids();
    StringBuffer itemName = new StringBuffer();
    itemName.append(itemset[0]);
    LinkedList<Item> curr = new LinkedList<Item>(Collections.singleton(item));
    
    for (int i = 1; i < itemset.length; i++) {
      item = singletons.get(indices.get(itemset[i]));
      tids = intersect(tids, item.getTids());
      itemName.append(" " + itemset[i]);
      curr.add(item);
    }
    
    item = new Item(itemName.toString().trim(), tids.length, tids);
    
    int sup = item.freq();
    int found = 0;
    
    if (sup >= minSup) {
      if (curr.size() >= minSize && curr.size() <= maxSize) {
        reporter.report(curr, sup, item.getTids());
      }
      found++;
    }
    
    // go down the tree if max size not yet reached
    if (curr.size() >= maxSize) {
      return found;
    }
    
    int indexLastItem = indices.get(itemset[itemset.length - 1]);
    ArrayList<Item> diffSets = new ArrayList<Item>(singletons.size() - indexLastItem);
    ListIterator<Item> it = singletons.listIterator(indexLastItem + 1);
    while (it.hasNext()) {
      Item nextItem = it.next();
      int[] diffSet;
      diffSet = setDifference(item, nextItem);
      if (sup - diffSet.length >= minSup) {
        diffSets.add(new Item(nextItem.name, sup - diffSet.length, diffSet));
      }
    }
    
    setupCost += System.currentTimeMillis() - beg;
    
    beg = System.currentTimeMillis();
    
    int i = 0;
    for (Iterator<Item> dIt = diffSets.iterator(); dIt.hasNext(); i++) {
      Item newItem = dIt.next();
      found += declat(sup, curr, i, diffSets, minSup);
      if (closureCheck(newItem)) {
        break;
      }
    }
    
    recursiveCost += System.currentTimeMillis() - beg;
    
    return found;
  }
  
  public void close() {
    reporter.close();
  }
  
  private int declat(int currSup, LinkedList<Item> curr, int index, List<Item> bitSets, int minSup) {
    if (index == bitSets.size()) {
      return 0;
    }
    
    boolean report = true;
    
    Item item = bitSets.get(index);
    int sup = currSup - item.getTids().length;
    
    List<Item> newBitSets = new ArrayList<Item>();
    if (curr.size() < maxSize) {
      
      for (Iterator<Item> it = bitSets.listIterator(index + 1); it.hasNext();) {
        Item nextItem = it.next();
        int[] condTids;
        // if (curr.isEmpty()) {
        // condTids = Utils.setDifference(item, nextItem);
        // } else {
        // condTids = Utils.setDifference(nextItem, item);
        // }
        condTids = Utils.setDifference(nextItem, item);
        int supOf = condTids.length;
        if (sup - supOf >= minSup) {
          newBitSets.add(new Item(nextItem.name, sup - supOf, condTids));
          
          // closed superset
          if (supOf == 0) {
            report &= false;
          }
        }
      }
    }
    
    curr.add(item);
    if (sup >= minSup) {
      report = (report && curr.size() >= minSize && curr.size() < maxSize);
      if (report) {
        reporter.report(curr, sup, item.getTids());
      }
    }
    int found = 1;
    if (curr.size() < maxSize) {
      int size = newBitSets.size();
      for (int i = 0; i < size; i++) {
        found += declat(sup, curr, i, newBitSets, minSup);
      }
    } else {
      Iterator<Item> it = curr.iterator();
      int[] tids;
      if (it.hasNext()) {
        final int[] firstTids = it.next().getTids();
        tids = Arrays.copyOf(firstTids, firstTids.length);
        
        while (it.hasNext()) {
          Item freqItem = it.next();
          item = singletons.get(indices.get(freqItem.name));
          tids = intersect(tids, item.getTids());
        }
        reporter.report(curr, sup, tids);
      }
    }
    curr.pollLast();
    return found;
  }
  
  private static List<Item> readSingletons(Configuration conf, Path path) throws IOException, URISyntaxException {
    SequenceFile.Reader r = new SequenceFile.Reader(FileSystem.get(new URI("file:///"), conf), path, conf);
    
    List<Item> bitSets = new ArrayList<Item>();
    
    Text key = new Text();
    IntArrayWritable value = new IntArrayWritable();
    
    while (r.next(key, value)) {
      Writable[] tidListsW = value.get();
      
      int[] tids = new int[tidListsW.length];
      
      for (int i = 0; i < tidListsW.length; i++) {
        tids[i] = ((IntWritable) tidListsW[i]).get();
      }
      
      bitSets.add(new Item(key.toString(), tids.length, tids));
    }
    r.close();
    
    return bitSets;
  }
  
  public static void main(String[] args) throws IOException {
    List<Item> items;
    int minSup = 1;
    try {
      items = readSingletons(new Configuration(), new Path(
          "/Users/Sandy/workspaces/hadoop/dist-eclat-imp/tmp1/tidlists/part-r-00000"));
      
      Collections.sort(items, new Comparator<Item>() {
        @Override
        public int compare(Item o1, Item o2) {
          return new Integer(o1.freq()).compareTo(o2.freq());
        }
      });
      
      Map<String,Integer> indexMap = new HashMap<String,Integer>();
      for (int i = 0; i < items.size(); i++) {
        indexMap.put(items.get(i).name, i);
      }
      
      EclatMiner miner = new EclatMiner();
      miner.setSetReporter(new SetReporter.TreeStringReporter(new PrintStream("outputFile.txt")));
      int i = 0;
      
      miner.mine(indexMap, items, items.get(i).name, minSup);
      miner.close();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }
}