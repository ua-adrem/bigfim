package ua.fim.disteclat.util;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import ua.util.Tools;

/**
 * Interface for different prefix distribution implementations. A distribution should divide the received prefixes in a
 * number of independent groups, while trying to optimize the load balancing.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public interface PrefixDistribution {
  
  /**
   * This class implements the Lowest Frequency prefix distribution. It sorts prefixes in descending order of frequency.
   * Then, when distributing prefixes, it continuously gives a new prefix to the group with the overall lowest support.
   * 
   * @author Sandy Moens & Emin Aksehirli
   */
  public class LowestFrequencyPDistribution implements PrefixDistribution {
    
    @Override
    public void sortPrefixes(List<ItemPair> prefixes, final Map<String,Integer> supMap) {
      Collections.sort(prefixes, new Comparator<ItemPair>() {
        @Override
        public int compare(ItemPair o1, ItemPair o2) {
          return -Tools.compare(o1.getSupport(), o2.getSupport());
        }
      });
    }
    
    @Override
    public void writeDistributionToOutput(int numberOfMappers, List<ItemPair> prefixes, MultipleOutputs<Text,Text> mos,
        String outputName) throws IOException, InterruptedException {
      int end = Math.min(numberOfMappers, prefixes.size());
      int[] count = new int[end];
      StringBuilder[] builders = new StringBuilder[end];
      for (int ix = 0; ix < end; ix++) {
        builders[ix] = new StringBuilder();
      }
      for (ItemPair prefix : prefixes) {
        int minIx = getLowestIndex(count);
        count[minIx] += prefix.support;
        builders[minIx].append(prefix.getText() + " ");
      }
      for (int ix = 0; ix < end; ix++) {
        mos.write(outputName, new Text("" + ix), new Text(builders[ix].substring(0, builders[ix].length() - 1)));
      }
    }
    
    private static int getLowestIndex(int[] count) {
      int min = count[0], minIx = 0;
      for (int ix = 1; ix < count.length; ix++) {
        int value = count[ix];
        if (value < min) {
          min = value;
          minIx = ix;
        }
      }
      return minIx;
    }
  }
  
  /**
   * This class implements the Round Robing prefix distribution. It sorts prefixes in ascending order of constituting
   * singletons. Then, it divides the prefixes among k mappers using the formula i%k=mId, where i is the index of the
   * prefix, and mId is the id of the mapper
   * 
   * @author Sandy Moens & Emin Aksehirli
   */
  public class RoundRobinPDistribution implements PrefixDistribution {
    @Override
    public void sortPrefixes(List<ItemPair> prefixes, final Map<String,Integer> supMap) {
      Collections.sort(prefixes, new Comparator<ItemPair>() {
        @Override
        public int compare(ItemPair o1, ItemPair o2) {
          int comp = 0;
          int ix = 0;
          
          while (comp == 0 && ix < o1.items.length) {
            int ii1Sup = supMap.get(o1.items[ix]);
            int ii2Sup = supMap.get(o2.items[ix]);
            comp = Tools.compare(ii1Sup, ii2Sup);
            ix++;
          }
          return comp;
        }
      });
      return;
    }
    
    @Override
    public void writeDistributionToOutput(int numberOfMappers, List<ItemPair> prefixes, MultipleOutputs<Text,Text> mos,
        String outputName) throws IOException, InterruptedException {
      int end = Math.min(numberOfMappers, prefixes.size());
      for (int ix = 0; ix < end; ix++) {
        StringBuilder builder = new StringBuilder();
        for (int ix1 = ix; ix1 < prefixes.size(); ix1 += numberOfMappers) {
          builder.append(prefixes.get(ix1).getText() + " ");
        }
        mos.write(outputName, new Text("" + ix), new Text(builder.substring(0, builder.length() - 1)));
      }
      return;
    }
  }
  
  /**
   * Class singleton or itemset and its support
   */
  public static class ItemPair {
    public final String[] items;
    public int support;
    
    public ItemPair(String[] items) {
      this.items = items;
    }
    
    public ItemPair(String[] text, int support) {
      this(text);
      this.support = support;
    }
    
    public String getText() {
      StringBuilder builder = new StringBuilder();
      for (String item : items) {
        builder.append(item + "_");
      }
      return builder.substring(0, builder.length());
    }
    
    public int getSupport() {
      return support;
    }
    
    @Override
    public String toString() {
      return getText() + " (" + support + ")";
    }
  }

  void sortPrefixes(List<ItemPair> prefixes, Map<String,Integer> supMap);
  
  public void writeDistributionToOutput(int numberOfMappers, List<ItemPair> prefixes, MultipleOutputs<Text,Text> mos,
      String outputName) throws IOException, InterruptedException;
  
}
