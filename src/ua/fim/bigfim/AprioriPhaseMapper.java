package ua.fim.bigfim;

import static org.apache.hadoop.filecache.DistributedCache.getLocalCacheFiles;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AprioriPhaseMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
  
  public static class Trie {
    public int id;
    public int support;
    public Map<Integer,Trie> children;
    
    public Trie(int id) {
      this.id = id;
      support = 0;
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
    
    public void incrementSupport() {
      this.support++;
    }
  }
  
  private static final String ItemDelimiter = " ";
  
  private Set<Integer> singletons;
  private Trie countTrie;
  
  private int phase = 1;
  
  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    
    Path[] localCacheFiles = getLocalCacheFiles(conf);
    countTrie = new Trie(-1);
    if (localCacheFiles != null) {
      String filename = localCacheFiles[0].toString();
      List<Set<Integer>> tmpWords = readWordsFromFile(filename);
      singletons = new HashSet<Integer>();
      Set<SortedSet<Integer>> words = createLengthPlusOneWords(tmpWords, singletons);
      
      phase = tmpWords.get(0).size() + 1;
      
      countTrie = initializeCountTrie(words);
      
      System.out.println("Singletons: " + singletons.size());
      System.out.println("Words: " + words.size());
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
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    List<Integer> items = convertLineToSet(line, phase == 1, singletons);
    incrementSubSets(items);
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    if (phase == 2) {
      System.out.println();
    }
    recReport(context, new StringBuilder(), countTrie);
  }
  
  private void recReport(Context context, StringBuilder builder, Trie trie) throws IOException, InterruptedException {
    int length = builder.length();
    for (Entry<Integer,Trie> entry : trie.children.entrySet()) {
      Trie recTrie = entry.getValue();
      builder.append(recTrie.id + " ");
      if (recTrie.children.isEmpty()) {
        Text key = new Text(builder.substring(0, builder.length() - 1));
        IntWritable value = new IntWritable(recTrie.support);
        context.write(key, value);
      } else {
        recReport(context, builder, recTrie);
      }
      builder.setLength(length);
    }
  }
  
  private void incrementSubSets(List<Integer> items) {
    if (items.size() < phase) {
      return;
    }
    
    if (phase == 1) {
      for (int i = 0; i < items.size(); i++) {
        Trie recTrie = countTrie.getChild(items.get(i));
        recTrie.incrementSupport();
      }
      return;
    }
    
    doRecursiveCount(items, 0, countTrie);
  }
  
  private void doRecursiveCount(List<Integer> items, int ix, Trie trie) {
    for (int i = ix; i < items.size(); i++) {
      Trie recTrie = trie.children.get(items.get(i));
      if (recTrie != null) {
        if (recTrie.children.isEmpty()) {
          recTrie.incrementSupport();
        } else {
          doRecursiveCount(items, i + 1, recTrie);
        }
      }
    }
  }
  
  public static List<Integer> convertLineToSet(String line, boolean levelOne, Set<Integer> singletons) {
    String[] itemsSplit = line.split(ItemDelimiter);
    
    List<Integer> items = new ArrayList<Integer>(itemsSplit.length);
    for (String itemString : itemsSplit) {
      int item = Integer.parseInt(itemString);
      if (!levelOne && !singletons.contains(item)) {
        continue;
      }
      items.add(item);
    }
    Collections.sort(items);
    return items;
  }
  
  public static Set<SortedSet<Integer>> createLengthPlusOneWords(List<Set<Integer>> tmpWords, Set<Integer> singletons) {
    Set<SortedSet<Integer>> words = new HashSet<SortedSet<Integer>>();
    
    int i = 0;
    ListIterator<Set<Integer>> it1 = tmpWords.listIterator(i);
    for (; i < tmpWords.size() - 1; i++) {
      Set<Integer> word1 = it1.next();
      ListIterator<Set<Integer>> it2 = tmpWords.listIterator(i + 1);
      while (it2.hasNext()) {
        SortedSet<Integer> set = new TreeSet<Integer>(word1);
        set.addAll(it2.next());
        if (set.size() == word1.size() + 1) {
          words.add(set);
          singletons.addAll(set);
        }
      }
    }
    
    return words;
  }
  
  public static List<Set<Integer>> readWordsFromFile(String string) throws NumberFormatException, IOException {
    List<Set<Integer>> words = new ArrayList<Set<Integer>>();
    
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(string));
    while ((line = reader.readLine()) != null) {
      String[] splits = line.split("\t")[0].split(" ");
      Set<Integer> set = new HashSet<Integer>(splits.length);
      for (String split : splits) {
        set.add(Integer.parseInt(split));
      }
      words.add(set);
    }
    reader.close();
    
    return words;
  }
}