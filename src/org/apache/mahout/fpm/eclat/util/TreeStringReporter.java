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

import static org.apache.mahout.fpm.eclat.util.TriePrinter.CLOSESUP;
import static org.apache.mahout.fpm.eclat.util.TriePrinter.OPENSUP;
import static org.apache.mahout.fpm.eclat.util.TriePrinter.SEPARATOR;
import static org.apache.mahout.fpm.eclat.util.TriePrinter.SYMBOL;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings({"unchecked", "rawtypes"})
public class TreeStringReporter implements SetReporter {
  private static final int MAX_SETS_BUFFER = 1000000;
  
  private final Context context;
  private final StringBuilder builder;
  
  private int[] prevSet;
  private int count;
  
  public TreeStringReporter(Context context) {
    this.context = context;
    builder = new StringBuilder();
    count = 0;
  }
  
  @Override
  public void report(int[] itemset, int support) {
    
    if (prevSet == null) {
      for (int i = 0; i < itemset.length - 1; i++) {
        builder.append(itemset[i]).append(SEPARATOR);
      }
    } else {
      int depth = 0;
      while (depth < itemset.length && depth < prevSet.length && itemset[depth] == prevSet[depth]) {
        depth++;
      }
      
      for (int i = prevSet.length - depth; i > 0; i--) {
        builder.append(SYMBOL);
      }
      for (int i = depth; i < itemset.length - 1; i++) {
        builder.append(itemset[i]).append(SEPARATOR);
      }
    }
    builder.append(itemset[itemset.length - 1]).append(OPENSUP).append(support).append(CLOSESUP);
    prevSet = Arrays.copyOf(itemset, itemset.length);
    count++;
    if (count % MAX_SETS_BUFFER == 0) {
      try {
        context.write(new Text("" + count), new Text(builder.toString()));
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println("wrote " + count + " compressed itemsets");
      builder.setLength(0);
      count = 0;
    }
  }
  
  @Override
  public void close() {
    try {
      context.write(new Text("" + count), new Text(builder.toString()));
      System.out.println("wrote " + count + " compressed itemsets");
      builder.setLength(0);
      count = 0;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}