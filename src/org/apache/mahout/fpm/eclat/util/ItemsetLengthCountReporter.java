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

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ItemsetLengthCountReporter implements SetReporter {
  
  private final Context context;
  ConcurrentMap<Integer,AtomicLong> counts = new ConcurrentHashMap<Integer,AtomicLong>();
  
  public ItemsetLengthCountReporter(Context context) {
    this.context = context;
  }
  
  @Override
  public void report(int[] itemset, int support) {
    int size = itemset.length;
    AtomicLong count = counts.putIfAbsent(size, new AtomicLong());
    if (count == null) {
      count = counts.get(size);
    }
    count.incrementAndGet();
  }
  
  @Override
  public void close() {
    try {
      for (Entry<Integer,AtomicLong> entry : counts.entrySet()) {
        context.write(new Text("" + entry.getKey()), new LongWritable(entry.getValue().get()));
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
  
  public long getCountForLevel(int level) {
    AtomicLong count = counts.get(level);
    if (count == null) {
      return 0;
    }
    return count.get();
  }
  
  public Set<Integer> getLevels() {
    return counts.keySet();
  }
}