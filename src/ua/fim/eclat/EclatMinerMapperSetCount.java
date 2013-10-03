package ua.fim.eclat;

import org.apache.hadoop.io.LongWritable;

import ua.fim.eclat.util.ItemsetLengthCountReporter;
import ua.fim.eclat.util.SetReporter;

public class EclatMinerMapperSetCount extends EclatMinerMapperBase<LongWritable> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new ItemsetLengthCountReporter(context);
  }
}