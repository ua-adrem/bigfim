/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mahout.fpm.disteclat;

import static java.lang.Integer.parseInt;
import static org.apache.hadoop.filecache.DistributedCache.getLocalCacheFiles;
import static org.apache.mahout.fpm.disteclat.DistEclatDriver.OSingletonsOrder;
import static org.apache.mahout.fpm.disteclat.DistEclatDriver.OSingletonsTids;
import static org.apache.mahout.fpm.disteclat.Utils.readSingletonsOrder;
import static org.apache.mahout.fpm.disteclat.Utils.readTidLists;
import static org.apache.mahout.fpm.util.Config.MIN_SUP_KEY;
import static org.apache.mahout.fpm.util.Config.PREFIX_LENGTH_KEY;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.fpm.eclat.EclatMiner;
import org.apache.mahout.fpm.eclat.util.Item;
import org.apache.mahout.fpm.eclat.util.PrefixItemTIDsReporter;
import org.apache.mahout.fpm.eclat.util.SetReporter;
import org.apache.mahout.fpm.hadoop.util.IntArrayWritable;

/**
 * This class implements the Mapper for the second MapReduce cycle for Dist-Eclat. It receives a list of singletons for
 * which it has to create X-FIs seeds by growing the lattice tree downwards. The ordering is retrieved through the
 * distributed cache.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class PrefixComputerMapper extends Mapper<LongWritable,Text,Text,IntArrayWritable> {
  
  public static final String Fis = "fis";
  public static final String Prefix = "prefix";
  public static final String Singleton = "singleton";
  public static final String Delimiter = "$";
  
  private List<Item> singletons;
  private Map<Integer,Integer> orderMap;
  private int minSup;
  private int prefixLength;
  
  @Override
  public void setup(Context context) throws IOException {
    try {
      Configuration conf = context.getConfiguration();
      
      minSup = conf.getInt(MIN_SUP_KEY, -1);
      prefixLength = conf.getInt(PREFIX_LENGTH_KEY, 1);
      
      Path[] localCacheFiles = getLocalCacheFiles(conf);
      
      for (Path path : localCacheFiles) {
        String pathString = path.toString();
        if (pathString.contains(OSingletonsTids)) {
          System.out.println("[PrefixComputerMapper]: Reading singletons");
          singletons = readTidLists(conf, path);
        } else if (pathString.contains(OSingletonsOrder)) {
          System.out.println("[PrefixComputerMapper]: Reading singleton orders");
          orderMap = readSingletonsOrder(path);
        }
      }
      
      sortSingletons();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] split = value.toString().split("\t");
    String items = split[1];
    
    // if the prefix length is 1, just report the singletons, otherwise use
    // Eclat to find X-FIs seeds
    EclatMiner miner = new EclatMiner();
    SetReporter reporter = new PrefixItemTIDsReporter(context, prefixLength, singletons, orderMap);
    
    miner.setSetReporter(reporter);
    miner.setMaxSize(prefixLength);
    miner.setMinSize(prefixLength);
    
    for (String itemStr : items.split(" ")) {
      final Item item = singletons.get(orderMap.get(Integer.valueOf(itemStr)));
      assert (item.id == parseInt(itemStr));
      miner.mineRecByPruning(item, singletons, minSup);
    }
  }
  
  /**
   * Sorts the singletons using the orderings retrieved from file
   */
  private void sortSingletons() {
    Collections.sort(singletons, new Comparator<Item>() {
      @Override
      public int compare(Item o1, Item o2) {
        Integer o1Rank = orderMap.get(o1.id);
        Integer o2Rank = orderMap.get(o2.id);
        return o1Rank.compareTo(o2Rank);
      }
    });
  }
}