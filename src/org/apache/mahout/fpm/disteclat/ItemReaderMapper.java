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

import static org.apache.mahout.fpm.util.Config.MIN_SUP_KEY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.fpm.hadoop.util.IntArrayWritable;

/**
 * This class implements the Mapper for the first MapReduce cycle for Dist-Eclat.It reads the database in vertical
 * format and reports only the frequent singletons. Each line in the database file should be formated as:
 * 'itemId<tab>comma-separated-tids'
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class ItemReaderMapper extends Mapper<LongWritable,Text,Text,IntArrayWritable> {
  
  public static final String tidsDelimiter = " ";
  public static final String tidsFileDelimiter = "\t";
  
  private int minSup;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    minSup = conf.getInt(MIN_SUP_KEY, 1);
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] split = value.toString().split(tidsFileDelimiter);
    String item = split[0];
    String[] tids = split[1].split(tidsDelimiter);
    
    if (tids.length < minSup) {
      return;
    }
    
    IntWritable[] iw = new IntWritable[tids.length];
    int ix = 0;
    for (String stringTid : tids) {
      iw[ix++] = new IntWritable(Integer.parseInt(stringTid));
    }
    
    context.write(new Text(item), new IntArrayWritable(iw));
  }
}