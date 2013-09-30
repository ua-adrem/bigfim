package ua.fim.bigfim;

import static java.lang.Integer.parseInt;
import static ua.fim.configuration.Config.PREFIX_LENGTH_KEY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EclatMinerSetCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
  
  long total = 0;
  
  int prefixLength;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    prefixLength = conf.getInt(PREFIX_LENGTH_KEY, 1);
  }
  
  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
    long numberOfSets = 0;
    for (LongWritable lw : values) {
      numberOfSets += lw.get();
    }
    total += numberOfSets;
    Text newKey = new Text(prefixLength + parseInt(key.toString()) + "");
    newKey = key;
    context.write(newKey, new LongWritable(numberOfSets));
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    context.write(new Text("Total"), new LongWritable(total));
  }
}