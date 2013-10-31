package org.apache.mahout.fpm.eclat;

import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.fpm.eclat.util.ItemsetLengthCountReporter;
import org.apache.mahout.fpm.eclat.util.SetReporter;

public class EclatMinerMapperSetCount extends EclatMinerMapperBase<LongWritable> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new ItemsetLengthCountReporter(context);
  }
}