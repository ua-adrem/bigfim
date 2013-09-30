package ua.fim.disteclat;

import static ua.fim.disteclat.util.Utils.getGroupString;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import ua.fim.disteclat.util.PrefixGroupReporter.Extension;
import ua.fim.disteclat.util.SetReporter.HadoopPerLevelCountReporter;

/**
 * This class implements the Mapper for the third MapReduce cycle for Dist-Eclat. It receives a list of prefixes that
 * have to be extended and mined to obtain frequent itemsets.This class only reports the number of itemsets that have
 * been found on each level for each prefix group.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class SubEclatMapperSetCount extends SubEclatMapperBase<LongWritable,Text,Text,LongWritable> {
  
  /*
   * ========================================================================
   * 
   * NON-STATIC
   * 
   * ========================================================================
   */
  
  @Override
  public void map(LongWritable key, Text value, Context context) {
    StringTokenizer st = new StringTokenizer(value.toString());
    st.nextToken();
    
    HadoopPerLevelCountReporter reporter = new HadoopPerLevelCountReporter(context);
    EclatMiner miner = new EclatMiner();
    miner.setSetReporter(reporter);
    
    int i = 0;
    int prefixes = st.countTokens();
    
    while (st.hasMoreElements()) {
      String item = st.nextToken();
      
      String group = getGroupString(item);
      List<Extension> extensions = prefixesGroups.get(group);
      long beg = System.currentTimeMillis();
      
      miner.mine(extensions, singletons, singletonsIndexMap, item, minSup);
      
      long time = System.currentTimeMillis() - beg;
      this.time += time;
      i++;
      System.out.println("Prefix: " + i + "/" + prefixes + ", time: " + time + ", avg time: " + 1.0 * this.time / i);
      
      printLevelInfo(reporter);
    }
    miner.close();
  }
  
  /**
   * Prints the cumulated number of itemsets generated for each level
   * 
   * @param reporter
   *          the reporter containing level information
   */
  private void printLevelInfo(HadoopPerLevelCountReporter reporter) {
    int maxLevel = reporter.getMaxLevel();
    StringBuilder builder = new StringBuilder();
    for (int level = 0; level < maxLevel; level++) {
      builder.append("Sets for level " + level + ": " + reporter.getCountForLevel(level) + "\n");
    }
    builder.append("=================================================");
    System.out.println(builder.toString());
  }
}