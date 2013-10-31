package ua.fim.bigfim;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import ua.fim.configuration.Config;

public class AprioriPhaseReducer extends Reducer<Text,IntWritable,Text,Writable> {
  
  private int minSup;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    minSup = conf.getInt(Config.MIN_SUP_KEY, 1);
  }
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int sup = 0;
    for (IntWritable localSup : values) {
      sup += localSup.get();
    }
    
    if (sup >= minSup) {
      context.write(key, new Text(sup + ""));
    }
  }
}