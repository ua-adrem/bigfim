package ua.fim.eclat;

import static ua.fim.configuration.Config.PREFIX_LENGTH_KEY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EclatMinerReducerSetCount extends Reducer<Text,LongWritable,Text,LongWritable> {
  
  private long total = 0;
  private int prefixLength;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    prefixLength = conf.getInt(PREFIX_LENGTH_KEY, 1);
  }
  
  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long levelTotal = 0;
    for (LongWritable lw : values) {
      levelTotal += lw.get();
    }
    total += levelTotal;
    context.write(key, new LongWritable(levelTotal));
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    System.out.println("Total: " + total);
    context.write(new Text("Total"), new LongWritable(total));
  }
}