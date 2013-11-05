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
package org.apache.mahout.fpm.bigfim;

import static org.apache.mahout.fpm.util.Config.MIN_SUP_KEY;
import static org.apache.mahout.fpm.util.Config.NUMBER_OF_MAPPERS_KEY;
import static org.apache.mahout.fpm.util.Config.SUBDB_SIZE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.fpm.hadoop.util.IntArrayWritable;

public class ComputeTidListReducer extends Reducer<Text,IntArrayWritable,IntArrayWritable,IntArrayWritable> {
  
  // max = 1 gb
  private static int MAX_FILE_SIZE = 1000000000;
  private static int MAX_NUMBER_OF_TIDS = (int) ((MAX_FILE_SIZE / 4) * 0.7);
  
  private final static IntArrayWritable EmptyIaw = new IntArrayWritable(new IntWritable[0]);
  
  private int minSup;
  private int subDbSize;
  
  private List<AtomicInteger> bucketSizes;
  
  private MultipleOutputs<IntArrayWritable,IntArrayWritable> mos;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    minSup = conf.getInt(MIN_SUP_KEY, 1);
    subDbSize = conf.getInt(SUBDB_SIZE, 1);
    int numberOfMappers = conf.getInt(NUMBER_OF_MAPPERS_KEY, 1);
    bucketSizes = new ArrayList<AtomicInteger>(numberOfMappers);
    for (int i = 0; i < numberOfMappers; i++) {
      bucketSizes.add(new AtomicInteger());
    }
    
    mos = new MultipleOutputs<IntArrayWritable,IntArrayWritable>(context);
    
  }
  
  @Override
  public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException,
      InterruptedException {
    Map<Integer,List<Integer>> map = new HashMap<Integer,List<Integer>>();
    for (IntArrayWritable iaw : values) {
      Writable[] w = iaw.get();
      int mapperId = ((IntWritable) w[0]).get();
      int item = ((IntWritable) w[1]).get();
      
      List<Integer> tids = map.get(item);
      if (tids == null) {
        tids = new ArrayList<Integer>();
        map.put(item, tids);
      }
      for (int i = 2; i < w.length; i++) {
        int subDbTid = ((IntWritable) w[i]).get();
        tids.add((mapperId * subDbSize) + subDbTid);
      }
    }
    
    int totalTids = 0;
    for (List<Integer> tids : map.values()) {
      if (tids.size() >= minSup) {
        totalTids += tids.size();
        Collections.sort(tids);
      }
    }
    if (totalTids > 0) {
      assignToBucket(key, map, totalTids);
    }
    
  }
  
  private void assignToBucket(Text key, Map<Integer,List<Integer>> map, int totalTids) throws IOException,
      InterruptedException {
    int lowestBucket = getLowestBucket();
    if (!checkLowestBucket(lowestBucket, totalTids)) {
      bucketSizes.add(new AtomicInteger());
      lowestBucket = bucketSizes.size() - 1;
    }
    bucketSizes.get(lowestBucket).addAndGet(totalTids);
    
    String baseOutputPath = "bucket-" + lowestBucket;
    mos.write(convert(key.toString()), EmptyIaw, baseOutputPath);
    for (Entry<Integer,List<Integer>> entry : map.entrySet()) {
      if (entry.getValue().size() >= minSup) {
        IntArrayWritable owKey = convert(entry.getKey());
        IntArrayWritable owValue = convert(entry.getValue());
        mos.write(owKey, owValue, baseOutputPath);
      }
    }
    mos.write(EmptyIaw, EmptyIaw, baseOutputPath);
  }
  
  private static boolean checkLowestBucket(int lowestBucket, int totalTids) {
    return (lowestBucket + totalTids) <= MAX_NUMBER_OF_TIDS;
  }
  
  private int getLowestBucket() {
    double min = Integer.MAX_VALUE;
    int id = -1;
    int ix = 0;
    for (AtomicInteger bucketSize : bucketSizes) {
      int bs = bucketSize.get();
      if (bs < min) {
        min = bs;
        id = ix;
      }
      ix++;
    }
    return id;
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    mos.close();
  }
  
  private static IntArrayWritable convert(String string) {
    String[] splits = string.split(" ");
    IntWritable[] iw = new IntWritable[splits.length];
    int i = 0;
    for (String split : splits) {
      iw[i++] = new IntWritable(Integer.parseInt(split));
    }
    return new IntArrayWritable(iw);
  }
  
  private static IntArrayWritable convert(Integer key) {
    IntWritable[] iw = new IntWritable[1];
    iw[0] = new IntWritable(key);
    return new IntArrayWritable(iw);
  }
  
  private static IntArrayWritable convert(List<Integer> tidsList) {
    IntWritable[] iw = new IntWritable[tidsList.size()];
    int i = 0;
    for (int tid : tidsList) {
      iw[i++] = new IntWritable(tid);
    }
    return new IntArrayWritable(iw);
  }
}