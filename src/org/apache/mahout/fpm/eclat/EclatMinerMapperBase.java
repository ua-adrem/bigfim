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
package org.apache.mahout.fpm.eclat;

import static org.apache.mahout.fpm.util.Config.MIN_SUP_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.fpm.eclat.util.Item;
import org.apache.mahout.fpm.eclat.util.SetReporter;
import org.apache.mahout.fpm.hadoop.util.IntArrayWritable;

public abstract class EclatMinerMapperBase<VALUEOUT> extends Mapper<IntArrayWritable,IntArrayWritable,Text,VALUEOUT> {
  
  private int minSup;
  
  protected abstract SetReporter getReporter(Context context);
  
  int[] prefix;
  List<Item> items;
  
  public EclatMinerMapperBase() {
    super();
  }
  
  @Override
  public void setup(Context context) throws IOException {
    Configuration conf = context.getConfiguration();
    
    minSup = conf.getInt(MIN_SUP_KEY, 1);
    
    items = new ArrayList<Item>();
  }
  
  @Override
  public void map(IntArrayWritable key, IntArrayWritable value, Context context) throws IOException,
      InterruptedException {
    Writable[] keyWritables = key.get();
    Writable[] valueWritables = value.get();
    
    if (keyWritables.length == 0 && valueWritables.length == 0) {
      mineSubTree(context);
      prefix = null;
      items.clear();
    } else if (valueWritables.length == 0) {
      prefix = new int[keyWritables.length];
      int i = 0;
      for (Writable w : keyWritables) {
        prefix[i++] = ((IntWritable) w).get();
      }
    } else {
      int item = ((IntWritable) keyWritables[0]).get();
      int[] tids = new int[valueWritables.length];
      int i = 0;
      for (Writable w : valueWritables) {
        tids[i++] = ((IntWritable) w).get();
      }
      items.add(new Item(item, tids.length, tids));
    }
  }
  
  @Override
  public void cleanup(Context context) {
    mineSubTree(context);
  }
  
  private void mineSubTree(Context context) {
    if (prefix == null || prefix.length == 0) {
      return;
    }
    Collections.sort(items, new EclatMiner.AscendingItemComparator());
    
    StringBuilder builder = new StringBuilder();
    builder.append("Run eclat: prefix: ");
    for (int p : prefix) {
      builder.append(p + " ");
    }
    builder.append("#items: " + items.size());
    System.out.println(builder.toString());
    EclatMiner miner = new EclatMiner();
    SetReporter reporter = getReporter(context);
    miner.setSetReporter(reporter);
    miner.mineRec(prefix, items, minSup);
    reporter.close();
  }
  
}