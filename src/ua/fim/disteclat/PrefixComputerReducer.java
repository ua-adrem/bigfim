package ua.fim.disteclat;

import static ua.fim.configuration.Config.NUMBER_OF_MAPPERS_KEY;
import static ua.fim.configuration.Config.PREFIX_LENGTH_KEY;
import static ua.fim.disteclat.DistEclatDriver.OPrefixesDistribution;
import static ua.fim.disteclat.PrefixComputerMapper.Fis;
import static ua.fim.disteclat.PrefixComputerMapper.Singleton;
import static ua.fim.disteclat.util.PrefixGroupReporter.DELIMITER_EXTENSIONS;
import static ua.fim.disteclat.util.PrefixGroupReporter.DELIMITER_SUBEXTENSIONSLIST;
import static ua.fim.disteclat.util.PrefixGroupReporter.DELIMITER_SUPPORT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import ua.fim.disteclat.util.PrefixDistribution;
import ua.fim.disteclat.util.PrefixGroupReporter;
import ua.util.Tools;

/**
 * This class implements the Reducer for the second MapReduce cycle for Dist-Eclat. It gets all singletons, frequent
 * itemsets to level X and all seeds. Singletons are used to sort the seeds based on ascending frequency of the
 * singletons they contain. The seeds are then distributed among the available number of mappers and written to disk.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class PrefixComputerReducer extends Reducer<Text,ObjectWritable,Text,Text> {
  
  /*
   * ========================================================================
   * 
   * STATIC
   * 
   * ========================================================================
   */
  
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
  
  /*
   * ========================================================================
   * 
   * NON-STATIC
   * 
   * ========================================================================
   */
  
  private int numberOfMappers;
  private int prefixLength;
  
  private List<ItemPair> prefixes;
  private Map<String,Integer> supMap;
  private PrefixDistribution pDistribution;
  
  private MultipleOutputs<Text,Text> mos;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    numberOfMappers = conf.getInt(NUMBER_OF_MAPPERS_KEY, 1);
    prefixLength = conf.getInt(PREFIX_LENGTH_KEY, 1);
    
    supMap = new HashMap<String,Integer>();
    prefixes = new LinkedList<ItemPair>();
    pDistribution = new PrefixDistribution.LowestFrequencyPDistribution();
    
    mos = new MultipleOutputs<Text,Text>(context);
    
  }
  
  @Override
  public void reduce(Text key, Iterable<ObjectWritable> values, Context context) throws IOException,
      InterruptedException {
    String keyString = key.toString();
    
    if (keyString.equals(Singleton)) {
      handleSingleton(values);
    } else if (keyString.equals(Fis)) {
      handleFis(values);
    } else if (keyString.startsWith(PrefixGroupReporter.PREFIX_GROUP)) {
      handlePrefixGroups(values, keyString.split(" ")[1]);
    } else {
      String err = "[PrefixComputerReducer]: Don't know how to handle: ";
      System.out.println(err + key.toString());
    }
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    if (prefixLength == 1) {
      createSingletonPrefixes();
    }
    
    pDistribution.sortPrefixes(prefixes, supMap);
    
    if (prefixLength == 1) {
      writeEmptyPrefixGroup();
    }
    
    pDistribution.writeDistributionToOutput(numberOfMappers, prefixes, mos, OPrefixesDistribution);
    
    mos.close();
  }
  
  /**
   * Parses a list of singletons with their support and puts them in the singletons support map.
   * 
   * @param values
   *          the list of singletons
   */
  private void handleSingleton(Iterable<ObjectWritable> values) {
    for (ObjectWritable value : values) {
      String[] split = ((String) value.get()).split("\\)");
      for (String item : split) {
        String[] itemSplit = item.split(" \\(");
        String[] items = itemSplit[0].split(" ");
        int support = Integer.parseInt(itemSplit[1]);
        ItemPair singleton = new ItemPair(items, support);
        supMap.put(singleton.items[0], singleton.support);
      }
    }
  }
  
  /**
   * Parses a list of frequent itemsets and writes them to disk.
   * 
   * @param values
   *          a list of frequent itemsets
   * @throws IOException
   * @throws InterruptedException
   */
  private void handleFis(Iterable<ObjectWritable> values) throws IOException, InterruptedException {
    StringBuilder builder = new StringBuilder();
    for (Iterator<ObjectWritable> it = values.iterator(); it.hasNext();) {
      builder.append((String) it.next().get());
    }
    mos.write(DistEclatDriver.OFises, new Text(), new Text(builder.substring(0, builder.length() - 1)));
  }
  
  /**
   * Parses a prefix group together with its extensions. The last extension can also contain a list of subextensions,
   * which means the former is actually a closed extension.
   * 
   * @param values
   *          the list of extensions
   * @param prefixString
   *          the prefix group
   * @throws IOException
   * @throws InterruptedException
   */
  private void handlePrefixGroups(Iterable<ObjectWritable> values, String prefixString) throws IOException,
      InterruptedException {
    String[] prefix = prefixString.split("_");
    for (ObjectWritable value : values) {
      // extract extensions
      String extensionsString = (String) value.get();
      String[] extensions = extensionsString.split(DELIMITER_EXTENSIONS);
      String last = extensions[extensions.length - 1];
      int support = 0;
      for (String extension : extensions) {
        String[] fis = Arrays.copyOf(prefix, prefix.length + 1);
        String[] fi = extension.split(DELIMITER_SUPPORT);
        fis[fis.length - 1] = fi[0];
        if (extension == last) {
          // check if last extension still has subsextensions. If yes,
          // the extension is closed
          String[] split = fi[1].split("\\" + DELIMITER_SUBEXTENSIONSLIST);
          support = Integer.parseInt(split[0]);
        } else {
          support = Integer.parseInt(fi[1]);
        }
        
        prefixes.add(new ItemPair(fis, support));
      }
      
      // write group to file
      mos.write(DistEclatDriver.OPrefixesGroups, new Text(prefixString), new Text((String) value.get()));
    }
  }
  
  /**
   * Creates a list of prefixes based on the frequent singletons and puts them into prefixes.
   */
  private void createSingletonPrefixes() {
    for (Entry<String,Integer> entry : supMap.entrySet()) {
      prefixes.add(new ItemPair(new String[] {entry.getKey()}, entry.getValue()));
    }
  }
  
  /**
   * Writes the empty prefix group. This is only used when prefixes of length 1 have been specified. The empty prefix
   * group consists of an empty prefix and all frequent singletons.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeEmptyPrefixGroup() throws IOException, InterruptedException {
    StringBuilder builder = new StringBuilder();
    for (ItemPair singleton : getSortedSingletonPrefixes()) {
      String name = singleton.getText();
      int support = singleton.support;
      builder.append(name.substring(0, name.length() - 1) + DELIMITER_SUPPORT + support + " ");
    }
    // write group to file
    mos.write(DistEclatDriver.OPrefixesGroups, new Text(""), new Text(builder.substring(0, builder.length() - 1)));
  }
  
  /**
   * Returns the sorted list of singleton prefixes in ascending order of frequency.
   * 
   * @return the sorted list of singletons
   */
  private List<ItemPair> getSortedSingletonPrefixes() {
    List<ItemPair> items = new ArrayList<ItemPair>(prefixes);
    Collections.sort(items, new Comparator<ItemPair>() {
      @Override
      public int compare(ItemPair o1, ItemPair o2) {
        int sup1 = o1.support;
        int sup2 = o2.support;
        return Tools.compare(sup1, sup2);
      }
    });
    return items;
  }
  
}