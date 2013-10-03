package ua.fim.disteclat;

import org.apache.hadoop.io.LongWritable;

import ua.fim.disteclat.util.SetReporter;
import ua.fim.disteclat.util.SetReporter.HadoopPerLevelCountReporter;

/**
 * This class implements the Mapper for the third MapReduce cycle for Dist-Eclat. It receives a list of prefixes that
 * have to be extended and mined to obtain frequent itemsets.This class only reports the number of itemsets that have
 * been found on each level for each prefix group.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class SubEclatMapperSetCount extends SubEclatMapperBase<LongWritable> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new HadoopPerLevelCountReporter(context);
  }
}