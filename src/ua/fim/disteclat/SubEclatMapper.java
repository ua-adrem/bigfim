package ua.fim.disteclat;

import static ua.fim.disteclat.util.Utils.getGroupString;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import ua.fim.disteclat.util.PrefixGroupReporter.Extension;
import ua.fim.disteclat.util.SetReporter;

/**
 * This class implements the Mapper for the third MapReduce cycle for Dist-Eclat. It receives a list of prefixes that
 * have to be extended and mined to obtain frequent itemsets.This class reports the mined itemsets as compressed tree
 * strings.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class SubEclatMapper extends SubEclatMapperBase<LongWritable,Text,Text,Text> {
  
  /*
   * ========================================================================
   * 
   * NON-STATIC
   * 
   * ========================================================================
   */
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer st = new StringTokenizer(value.toString());
    st.nextToken();
    
    int i = 0;
    int prefixes = st.countTokens();
    
    while (st.hasMoreElements()) {
      SetReporter reporter = new SetReporter.HadoopTreeStringReporter(context);
      EclatMiner miner = new EclatMiner();
      miner.setSetReporter(reporter);
      String item = st.nextToken();
      
      String group = getGroupString(item);
      List<Extension> extensions = prefixesGroups.get(group);
      long beg = System.currentTimeMillis();
      
      miner.mine(extensions, singletons, singletonsIndexMap, item, minSup);
      
      long time = System.currentTimeMillis() - beg;
      this.time += time;
      i++;
      System.out.println("Prefix: " + i + "/" + prefixes + ", time: " + time + ", avg time: " + 1.0 * this.time / i);
      
      miner.close();
    }
  }
}
