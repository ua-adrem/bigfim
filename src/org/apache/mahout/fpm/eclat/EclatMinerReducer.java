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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This class implements the Reducer of the third MapReduce cycle for Dist-Eclat. It accumulates all itemsets reported
 * as compressed tree strings and writes them to file.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class EclatMinerReducer extends Reducer<Text,Text,Text,Text> {
  
  private long setsFound = 0;
  
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    long numberOfSets = Long.parseLong(key.toString());
    for (Text item : values) {
      setsFound += numberOfSets;
      context.write(key, item);
    }
  }
  
  @Override
  public void cleanup(Context context) {
    System.out.println("Mined " + setsFound + " itemsets");
  }
}