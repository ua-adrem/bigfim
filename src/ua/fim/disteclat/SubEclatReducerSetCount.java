package ua.fim.disteclat;

import static ua.fim.configuration.Config.PREFIX_LENGTH_KEY;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This class implements the Reducer of the third MapReduce cycle for Dist-Eclat. It accumulates the level counts for
 * each of the levels and reports the total sum for each level.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class SubEclatReducerSetCount extends Reducer<Text,LongWritable,Text,LongWritable> {
  
  /*
   * ========================================================================
   * 
   * NON-STATIC
   * 
   * ========================================================================
   */
  
  private long total = 0;
  private int prefixLength;
  
  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    
    prefixLength = conf.getInt(PREFIX_LENGTH_KEY, 1);
  }
  
  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    Iterator<LongWritable> it = values.iterator();
    long levelTotal = 0;
    while (it.hasNext()) {
      levelTotal += it.next().get();
    }
    total += levelTotal;
    int theLevel = prefixLength - 1 + (Integer.parseInt(key.toString()));
    context.write(new Text("" + theLevel), new LongWritable(levelTotal));
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    System.out.println("Total: " + total);
    context.write(new Text("Total"), new LongWritable(total));
  }
}