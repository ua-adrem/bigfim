package ua.fim.disteclat;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This class implements the Reducer of the third MapReduce cycle for Dist-Eclat. It accumulates all itemsets reported
 * as compressed tree strings and writes them to file.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class SubEclatReducer extends Reducer<Text,Text,Text,Text> {
  
  private long setsFound = 0;
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    long numberOfSets = Long.parseLong(key.toString());
    for (Text item : values) {
      setsFound += numberOfSets;
      context.write(key, item);
    }
  }
  
  @Override
  public void cleanup(Context context) {
    System.out.println("Mined " + setsFound + " itemsets");
  }
}