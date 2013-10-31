package org.apache.mahout.fpm.disteclat;

import static java.lang.Integer.parseInt;
import static org.apache.mahout.fpm.disteclat.DistEclatDriver.OSingletonsDistribution;
import static org.apache.mahout.fpm.disteclat.DistEclatDriver.OSingletonsOrder;
import static org.apache.mahout.fpm.disteclat.DistEclatDriver.OSingletonsTids;
import static org.apache.mahout.fpm.util.Config.NUMBER_OF_MAPPERS_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.fpm.hadoop.util.IntArrayWritable;

/**
 * This class implements the Reducer for the first MapReduce cycle for Dist-Eclat. It receives the complete set of
 * frequent singletons from the different mappers. The frequent singletons are sorted on ascending frequency and
 * distributed among a number of map-tasks.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class ItemReaderReducer extends Reducer<Text,IntArrayWritable,Text,Writable> {
  
  public static final Text EmptyKey = new Text("");
  
  private int numberOfMappers;
  private final Map<String,Integer> itemSupports = new HashMap<String,Integer>();
  
  private MultipleOutputs<Text,Writable> mos;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    mos = new MultipleOutputs<Text,Writable>(context);
    numberOfMappers = parseInt(conf.get(NUMBER_OF_MAPPERS_KEY, "1"));
  }
  
  @Override
  public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException,
      InterruptedException {
    // should only get one, otherwise duplicated item in database
    IntArrayWritable iaw = values.iterator().next();
    itemSupports.put(key.toString(), iaw.get().length);
    
    // write the item with the tidlist
    mos.write(OSingletonsTids, key, iaw);
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    List<String> sortedSingletons = getSortedSingletons();
    
    context.setStatus("Writing Singletons");
    writeSingletonsOrders(sortedSingletons);
    
    context.setStatus("Distributing Singletons");
    writeSingletonsDistribution(sortedSingletons);
    
    context.setStatus("Finished");
    mos.close();
  }
  
  /**
   * Gets the list of singletons in ascending order of frequency.
   * 
   * @return the sorted list of singletons
   */
  private List<String> getSortedSingletons() {
    List<String> items = new ArrayList<String>(itemSupports.keySet());
    
    Collections.sort(items, new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        Integer o1Supp = itemSupports.get(o1);
        Integer o2Supp = itemSupports.get(o2);
        return o1Supp.compareTo(o2Supp);
      }
    });
    
    return items;
  }
  
  /**
   * Writes the singletons order to the file OSingletonsOrder.
   * 
   * @param sortedSingletons
   *          the sorted singletons
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeSingletonsOrders(List<String> sortedSingletons) throws IOException, InterruptedException {
    StringBuilder builder = new StringBuilder();
    for (String singleton : sortedSingletons) {
      builder.append(singleton + " ");
    }
    
    Text order = new Text(builder.substring(0, builder.length() - 1));
    mos.write(OSingletonsOrder, EmptyKey, order);
  }
  
  /**
   * Writes the singletons distribution to file OSingletonsDistribution. The distribution is obtained using Round-Robin
   * allocation.
   * 
   * @param sortedSingletons
   *          the sorted list of singletons
   * @throws IOException
   * @throws InterruptedException
   */
  private void writeSingletonsDistribution(List<String> sortedSingletons) throws IOException, InterruptedException {
    int end = Math.min(numberOfMappers, sortedSingletons.size());
    
    Text mapperId = new Text();
    Text assignedItems = new Text();
    
    // Round robin assignment
    for (int ix = 0; ix < end; ix++) {
      StringBuilder builder = new StringBuilder();
      for (int ix1 = ix; ix1 < sortedSingletons.size(); ix1 += numberOfMappers) {
        builder.append(sortedSingletons.get(ix1) + " ");
      }
      
      mapperId.set("" + ix);
      assignedItems.set(builder.substring(0, builder.length() - 1));
      mos.write(OSingletonsDistribution, mapperId, assignedItems);
    }
  }
}