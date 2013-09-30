package ua.fim.bigfim;

import static org.apache.hadoop.filecache.DistributedCache.getLocalCacheFiles;
import static ua.fim.bigfim.AprioriPhaseMapper.createLengthPlusOneWords;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ua.hadoop.util.IntArrayWritable;

public class ComputeTidListMapper extends Mapper<LongWritable,Text,Text,IntArrayWritable> {
  
  public static int TIDS_BUFFER_SIZE = 1000000;
  
  public static class Trie {
    public int id;
    public List<Integer> tids;
    public Map<Integer,Trie> children;
    
    public Trie(int id) {
      this.id = id;
      tids = new LinkedList<Integer>();
      children = new HashMap<Integer,Trie>();
    }
    
    public Trie getChild(int id) {
      Trie child = children.get(id);
      if (child == null) {
        child = new Trie(id);
        children.put(id, child);
      }
      return child;
    }
    
    public void addTid(int tid) {
      tids.add(tid);
    }
  }
  
  private Text key;
  private IntArrayWritable iaw;
  
  private Set<Integer> singletons;
  private Trie countTrie;
  
  private int phase = 1;
  
  int counter;
  int tidCounter = 0;
  int id;
  
  public IntWritable[] createIntWritableWithIdSet(int numberOfTids) {
    IntWritable[] iw = new IntWritable[numberOfTids + 2];
    iw[0] = new IntWritable(id);
    return iw;
  }
  
  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    
    Path[] localCacheFiles = getLocalCacheFiles(conf);
    if (localCacheFiles != null) {
      String filename = localCacheFiles[0].toString();
      List<Set<Integer>> tmpWords = AprioriPhaseMapper.readWordsFromFile(filename);
      singletons = new HashSet<Integer>();
      Set<SortedSet<Integer>> words = createLengthPlusOneWords(tmpWords, singletons);
      
      countTrie = initializeCountTrie(words);
      
      phase = tmpWords.get(0).size() + 1;
    }
    counter = 0;
    id = context.getTaskAttemptID().getTaskID().getId();
    
    key = new Text();
    iaw = new IntArrayWritable();
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    List<Integer> items = AprioriPhaseMapper.convertLineToSet(line, phase == 1, singletons);
    reportItemTids(context, items);
    counter++;
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    if (tidCounter != 0) {
      doRecursiveReport(context, new StringBuilder(), countTrie);
    }
  }
  
  private static Trie initializeCountTrie(Set<SortedSet<Integer>> words) {
    Trie countTrie = new Trie(-1);
    for (SortedSet<Integer> word : words) {
      Trie trie = countTrie;
      Iterator<Integer> it = word.iterator();
      while (it.hasNext()) {
        trie = trie.getChild(it.next());
      }
    }
    return countTrie;
  }
  
  private void reportItemTids(Context context, List<Integer> items) throws IOException, InterruptedException {
    if (items.size() < phase) {
      return;
    }
    
    if (phase == 1) {
      for (Integer word : items) {
        countTrie.getChild(word).addTid(counter);
        tidCounter++;
      }
    } else {
      doRecursiveTidAdd(context, items, 0, countTrie);
    }
    if (tidCounter >= TIDS_BUFFER_SIZE) {
      System.out.println("Tids buffer reached, reporting " + tidCounter + " partial tids");
      doRecursiveReport(context, new StringBuilder(), countTrie);
      tidCounter = 0;
    }
  }
  
  private void doRecursiveTidAdd(Context context, List<Integer> items, int ix, Trie trie) throws IOException,
      InterruptedException {
    for (int i = ix; i < items.size(); i++) {
      Trie recTrie = trie.children.get(items.get(i));
      if (recTrie != null) {
        if (recTrie.children.isEmpty()) {
          recTrie.addTid(counter);
          tidCounter++;
        } else {
          doRecursiveTidAdd(context, items, i + 1, recTrie);
        }
      }
    }
  }
  
  private void doRecursiveReport(Context context, StringBuilder builder, Trie trie) throws IOException,
      InterruptedException {
    int length = builder.length();
    for (Trie recTrie : trie.children.values()) {
      if (recTrie != null) {
        if (recTrie.children.isEmpty()) {
          if (recTrie.tids.isEmpty()) {
            continue;
          }
          Text key = new Text(builder.substring(0, builder.length() - 1));
          IntWritable[] iw = createIntWritableWithIdSet(recTrie.tids.size());
          int i1 = 1;
          iw[i1++] = new IntWritable(recTrie.id);
          for (int tid : recTrie.tids) {
            iw[i1++] = new IntWritable(tid);
          }
          iaw.set(iw);
          context.write(key, iaw);
          recTrie.tids.clear();
        } else {
          builder.append(recTrie.id + " ");
          doRecursiveReport(context, builder, recTrie);
        }
      }
      builder.setLength(length);
    }
  }
  
  public static String setToMinusOneString(Set<Integer> set) {
    StringBuilder builder = new StringBuilder();
    Iterator<Integer> it = set.iterator();
    for (int i = 0; i < set.size() - 1; i++) {
      builder.append(it.next() + " ");
    }
    return builder.substring(0, builder.length() - 1);
  }
}