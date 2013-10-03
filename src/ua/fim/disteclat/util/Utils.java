package ua.fim.disteclat.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import ua.hadoop.util.IntArrayWritable;
import ua.util.Tools;

public class Utils {
  
  public static List<Item> readTidLists(Configuration conf, Path path) throws IOException, URISyntaxException {
    SequenceFile.Reader r = new SequenceFile.Reader(FileSystem.get(new URI("file:///"), conf), path, conf);
    
    List<Item> bitSets = new ArrayList<Item>();
    
    Text key = new Text();
    IntArrayWritable value = new IntArrayWritable();
    
    while (r.next(key, value)) {
      Writable[] tidListsW = value.get();
      
      int[] tids = new int[tidListsW.length];
      
      for (int i = 0; i < tidListsW.length; i++) {
        tids[i] = ((IntWritable) tidListsW[i]).get();
      }
      
      bitSets.add(new Item(key.toString(), tids.length, tids));
    }
    r.close();
    
    return bitSets;
  }
  
  
  public static Map<String,Integer> readSingletonsOrder(Path path) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
    
    String order = reader.readLine().trim();
    reader.close();
    
    Map<String,Integer> orderMap = new HashMap<String,Integer>();
    String[] split = order.split(" ");
    int ix = 0;
    for (String item : split) {
      orderMap.put(item, ix++);
    }
    return orderMap;
  }
  
  public static int[] intersect(Item item1, Item item2) {
    return Tools.intersect(item1.tids, item2.tids);
  }
  
  public static int[] setDifference(Item item1, Item item2) {
    return Tools.setDifference(item1.tids, item2.tids);
  }
}
