package ua.fim.bigfim;

import org.apache.hadoop.io.LongWritable;

import ua.fim.bigfim.util.SetReporter;

public class EclatMinerMapperSetCount extends EclatMinerMapperBase<LongWritable> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new SetReporter.HadoopPerLevelCountReporter(context);
  }
}