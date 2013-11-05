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

import static org.easymock.EasyMock.createMock;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.fpm.AllTests;
import org.easymock.EasyMock;
import org.junit.Test;

public class AprioriPhaseReducerTest {
  
  @Test
  public void reduce_No_Input() {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    
    EasyMock.replay(ctx);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void reduce_With_Input_Empty_MinSup_1() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    
    EasyMock.replay(ctx);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    AllTests.setField(reducer, "minSup", 1);
    
    reducer.reduce(new Text("1"), creatList(new int[] {}), ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void reduce_With_Input_MinSup_1() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    
    ctx.write(new Text("1"), new Text("10"));
    ctx.write(new Text("2"), new Text("7"));
    ctx.write(new Text("3"), new Text("11"));
    ctx.write(new Text("4"), new Text("1"));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    AllTests.setField(reducer, "minSup", 1);
    
    reducer.reduce(new Text("1"), creatList(new int[] {1, 2, 3, 4}), ctx);
    reducer.reduce(new Text("2"), creatList(new int[] {5, 2}), ctx);
    reducer.reduce(new Text("3"), creatList(new int[] {2, 4, 5}), ctx);
    reducer.reduce(new Text("4"), creatList(new int[] {1}), ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void reduce_With_Input_MinSup_5() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    
    ctx.write(new Text("1"), new Text("10"));
    ctx.write(new Text("2"), new Text("7"));
    ctx.write(new Text("3"), new Text("11"));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    AllTests.setField(reducer, "minSup", 5);
    
    reducer.reduce(new Text("1"), creatList(new int[] {1, 2, 3, 4}), ctx);
    reducer.reduce(new Text("2"), creatList(new int[] {5, 2}), ctx);
    reducer.reduce(new Text("3"), creatList(new int[] {2, 4, 5}), ctx);
    reducer.reduce(new Text("4"), creatList(new int[] {1}), ctx);
    
    EasyMock.verify(ctx);
  }
  
  @Test
  public void reduce_With_Input_MinSup_10() throws Exception {
    AprioriPhaseReducer.Context ctx = createMock(Reducer.Context.class);
    
    ctx.write(new Text("1"), new Text("10"));
    ctx.write(new Text("3"), new Text("11"));
    
    EasyMock.replay(ctx);
    
    AprioriPhaseReducer reducer = new AprioriPhaseReducer();
    AllTests.setField(reducer, "minSup", 10);
    
    reducer.reduce(new Text("1"), creatList(new int[] {1, 2, 3, 4}), ctx);
    reducer.reduce(new Text("2"), creatList(new int[] {5, 2}), ctx);
    reducer.reduce(new Text("3"), creatList(new int[] {2, 4, 5}), ctx);
    reducer.reduce(new Text("4"), creatList(new int[] {1}), ctx);
    
    EasyMock.verify(ctx);
  }
  
  private Iterable<IntWritable> creatList(int[] is) {
    List<IntWritable> list = new LinkedList<IntWritable>();
    for (int i : is) {
      list.add(new IntWritable(i));
    }
    return list;
  }
}
