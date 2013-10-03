package ua.fim.disteclat;

import static ua.fim.configuration.Config.MIN_SUP_KEY;
import static ua.fim.disteclat.DistEclatDriver.rExt;
import static ua.fim.disteclat.util.Utils.getGroupString;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ua.fim.disteclat.util.Item;
import ua.fim.disteclat.util.PrefixGroupReporter.Extension;
import ua.fim.disteclat.util.SetReporter;
import ua.fim.disteclat.util.Utils;

/**
 * This class provides base functionality for other SubEclatMappers. It takes care of setting up all necessary Objects
 * and structures, such that mining can be executed.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public abstract class SubEclatMapperBase<VALUEOUT> extends Mapper<LongWritable,Text,Text,VALUEOUT> {
  
  protected List<Item> singletons;
  protected Map<String,Integer> singletonsIndexMap;
  protected Map<String,List<Extension>> prefixesGroups;
  
  protected int minSup;
  protected long time;
  
  @Override
  public void setup(Context context) throws IOException {
    try {
      Configuration conf = context.getConfiguration();
      
      minSup = conf.getInt(MIN_SUP_KEY, -1);
      
      Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(conf);
      
      for (Path path : localCacheFiles) {
        if (path.toString().contains(DistEclatDriver.OSingletonsTids)) {
          System.out.println("[SubEclatMapperSetCount]: Reading singletons");
          singletons = Utils.readTidLists(conf, path);
        } else if (path.toString().endsWith(DistEclatDriver.OPrefixesGroups + rExt)) {
          System.out.println("[SubEclatMapperSetCount]: Reading reading prefix groups");
          prefixesGroups = Utils.readPrefixesGroups(conf, path);
          System.out.println(prefixesGroups);
        }
      }
      
      singletonsIndexMap = new HashMap<String,Integer>();
      for (Item item : singletons) {
        singletonsIndexMap.put(item.name, singletonsIndexMap.size());
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    }
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) {
    StringTokenizer st = new StringTokenizer(value.toString());
    st.nextToken();
    
    int i = 0;
    int prefixes = st.countTokens();
    
    while (st.hasMoreElements()) {
      SetReporter reporter = getReporter(context);
      EclatMiner miner = new EclatMiner();
      miner.setSetReporter(reporter);
      String item = st.nextToken();
      
      String group = getGroupString(item);
      List<Extension> extensions = prefixesGroups.get(group);
      long beg = System.currentTimeMillis();
      
      miner.mine(extensions, singletonsIndexMap, singletons, item, minSup);
      
      long localTime = System.currentTimeMillis() - beg;
      this.time += localTime;
      i++;
      System.out.println("Prefix: " + i + "/" + prefixes + ", time: " + localTime + ", avg time: " + 1.0 * this.time
          / i);
      
      miner.close();
    }
  }
  
  protected abstract SetReporter getReporter(Context context);
  
}