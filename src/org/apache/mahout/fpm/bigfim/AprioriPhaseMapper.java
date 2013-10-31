package org.apache.mahout.fpm.bigfim;

import static org.apache.hadoop.filecache.DistributedCache.getLocalCacheFiles;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
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
import org.apache.mahout.fpm.util.Trie;

/**
 * Mapper class for Apriori phase of BigFim. In this phase a list of base words are combined in words of length+1. The
 * latter are counted in the map function. If no base words are specified, all singletons are counted.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class AprioriPhaseMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
  
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
      List<Set<Integer>> tmpWords = readItemsetsFromFile(filename);
      Set<SortedSet<Integer>> words = createLengthPlusOneItemsets(tmpWords);
      singletons = getSingletonsFromWords(words);
      
      phase = tmpWords.get(0).size() + 1;
      
      countTrie = initializeCountTrie(words);
      
      System.out.println("Singletons: " + singletons.size());
      System.out.println("Words: " + words.size());
    }
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
      Integer item = Integer.valueOf(itemString);
      if (!levelOne && !singletons.contains(item)) {
        continue;
      }
      items.add(item);
    }
    Collections.sort(items);
    return items;
  }
  
  /**
   * Creates words of length equal to length+1
   * 
   * @param tmpItemsets
   *          words of length that are merged together to form length+1 words
   * @return
   */
  public static Set<SortedSet<Integer>> createLengthPlusOneItemsets(List<Set<Integer>> tmpItemsets) {
    Set<SortedSet<Integer>> itemsets = new HashSet<SortedSet<Integer>>();
    
    int i = 0;
    ListIterator<Set<Integer>> it1 = tmpItemsets.listIterator(i);
    for (; i < tmpItemsets.size() - 1; i++) {
      Set<Integer> itemset1 = it1.next();
      ListIterator<Set<Integer>> it2 = tmpItemsets.listIterator(i + 1);
      while (it2.hasNext()) {
        SortedSet<Integer> set = new TreeSet<Integer>(itemset1);
        set.addAll(it2.next());
        if (set.size() == itemset1.size() + 1) {
          itemsets.add(set);
        }
      }
    }
    
    return itemsets;
  }
  
  /**
   * Gets the unique list of singletons in a collection of words
   * 
   * @param words
   *          the collection of words
   * @return list of unique singletons
   */
  public static Set<Integer> getSingletonsFromWords(Set<SortedSet<Integer>> words) {
    Set<Integer> singletons = new HashSet<Integer>();
    for (SortedSet<Integer> word : words) {
      singletons.addAll(word);
    }
    return singletons;
  }
  
  public static List<Set<Integer>> readItemsetsFromFile(String string) throws NumberFormatException, IOException {
    List<Set<Integer>> itemsets = new ArrayList<Set<Integer>>();
    
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(string));
    while ((line = reader.readLine()) != null) {
      String[] splits = line.split("\t")[0].split(" ");
      Set<Integer> set = new HashSet<Integer>(splits.length);
      for (String split : splits) {
        set.add(Integer.valueOf(split));
      }
      itemsets.add(set);
    }
    reader.close();
    
    return itemsets;
  }
}