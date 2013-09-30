package ua.fim.bigfim;

import static ua.fim.configuration.Config.MIN_SUP_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import ua.fim.bigfim.util.Item;
import ua.fim.bigfim.util.SetReporter;
import ua.hadoop.util.IntArrayWritable;

public class EclatMinerSetCountMapper extends Mapper<IntArrayWritable,IntArrayWritable,Text,LongWritable> {
  
  private Text key;
  private IntArrayWritable iaw;
  private IntWritable[] iw;
  
  private int minSup;
  
  int[] prefix;
  List<Item> items;
  
  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    
    minSup = conf.getInt(MIN_SUP_KEY, 1);
    
    items = new ArrayList<Item>();
  }
  
  @Override
  public void map(IntArrayWritable key, IntArrayWritable value, Context context) throws IOException,
      InterruptedException {
    Writable[] keyWritables = key.get();
    Writable[] valueWritables = value.get();
    
    if (keyWritables.length == 0 && valueWritables.length == 0) {
      mineSubTree(context);
      prefix = null;
      items.clear();
    }
    if (valueWritables.length == 0) {
      prefix = new int[keyWritables.length];
      int i = 0;
      for (Writable w : keyWritables) {
        prefix[i++] = ((IntWritable) w).get();
      }
    } else {
      int item = ((IntWritable) keyWritables[0]).get();
      int[] tids = new int[valueWritables.length];
      int i = 0;
      for (Writable w : valueWritables) {
        tids[i++] = ((IntWritable) w).get();
      }
      items.add(new Item(item, tids.length, tids));
    }
  }
  
  @Override
  public void cleanup(Context context) {
    mineSubTree(context);
  }
  
  private void mineSubTree(Context context) {
    if (prefix == null) {
      return;
    }
    Collections.sort(items, new EclatMiner.AscendingItemComparator());
    
    StringBuilder builder = new StringBuilder();
    builder.append("Run eclat: prefix: ");
    for (int p : prefix) {
      builder.append(p + " ");
    }
    builder.append("#items: " + items.size());
    System.out.println(builder.toString());
    EclatMiner miner = new EclatMiner();
    SetReporter.HadoopPerLevelCountReporter reporter = new SetReporter.HadoopPerLevelCountReporter(context);
    miner.setSetReporter(reporter);
    miner.mineRec(prefix, items, minSup);
    reporter.close();
  }
}