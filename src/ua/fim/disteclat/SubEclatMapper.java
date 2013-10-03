package ua.fim.disteclat;

import org.apache.hadoop.io.Text;

import ua.fim.disteclat.util.SetReporter;

/**
 * This class implements the Mapper for the third MapReduce cycle for Dist-Eclat. It receives a list of prefixes that
 * have to be extended and mined to obtain frequent itemsets.This class reports the mined itemsets as compressed tree
 * strings.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class SubEclatMapper extends SubEclatMapperBase<Text> {
  
  @Override
  protected SetReporter getReporter(Context context) {
    return new SetReporter.HadoopTreeStringReporter(context);
  }
}
