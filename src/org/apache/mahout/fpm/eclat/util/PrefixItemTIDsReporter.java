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
package org.apache.mahout.fpm.eclat.util;

import static org.apache.mahout.fpm.util.Tools.intersect;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.mahout.fpm.hadoop.util.IntArrayWritable;

//@SuppressWarnings({"rawtypes", "unchecked"})
public class PrefixItemTIDsReporter implements SetReporter {
  
  private Context context;
  private int prefixLength;
  private List<Item> singletons;
  private Map<Integer,Integer> orderMap;
  
  
  public PrefixItemTIDsReporter(Context context, int prefixLength, List<Item> singletons, Map<Integer,Integer> orderMap) {
    this.context = context;
    this.prefixLength = prefixLength;
    this.singletons = singletons;
    this.orderMap = orderMap;
  }
  
  @Override
  public void report(int[] itemset, int support) {
    StringBuilder sb = new StringBuilder();
    if (itemset.length < prefixLength) {
      System.out.println("Found a short fis:" + Arrays.toString(itemset));
      return;
    }
    int prefixStrLength = 0;
    int lastItem = -1;
    for (int item : itemset) {
      prefixStrLength = sb.length() - 1;
      sb.append(item).append(" ");
      lastItem = item;
    }
    sb.setLength(prefixStrLength);
    
    Text key = new Text(sb.toString());
    
    int[] tids = computeTids(itemset);
    
    IntWritable[] iw = new IntWritable[tids.length + 1];
    
    for (int i = 1; i < iw.length; i++) {
      iw[i] = new IntWritable(tids[i - 1]);
    }
    iw[0] = new IntWritable(lastItem);
    
    try {
      context.write(key, new IntArrayWritable(iw));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private int[] computeTids(int[] itemset) {
    final int[] firstTids = singletons.get(orderMap.get(itemset[0])).getTids();
    int[] tids = Arrays.copyOf(firstTids, firstTids.length);
    
    for (int i = 1; i < itemset.length; i++) {
      Item item = singletons.get(orderMap.get(itemset[i]));
      tids = intersect(tids, item.getTids());
    }
    return tids;
  }
  
  @Override
  public void close() {
    
  }
  
}