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

import static org.junit.Assert.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import org.junit.Before;
import org.junit.Test;

public class TriePrinterTest {
  
  private OutputStream out;
  
  @Before
  public void setUp() {
    out = new ByteArrayOutputStream();
    TriePrinter.out = new PrintStream(out, true);
  }
  
  @Test
  public void prints_Supports_In_Paranthesis() {
    String str = "1 2 30(12)";
    String[] actuals = getPrintOut(str);
    
    final String[] expecteds = new String[] {"1 2 30 (12)"};
    
    assertArrayEquals(expecteds, actuals);
  }
  
  @Test
  public void pipes_Are_The_Separators_For_Closed_Items() {
    String str = "1 2 30(12)8|9|19(10)";
    String[] actuals = getPrintOut(str);
    
    final String[] expecteds = new String[] {"1 2 30 (12)", "1 2 30 8 9 19 (10)"};
    
    assertArrayEquals(expecteds, actuals);
  }
  
  @Test
  public void dollar_Means_One_Level_Up() {
    String str = "1 2 30(12)8|9(10)$5(11)";
    String[] actuals = getPrintOut(str);
    
    final String[] expecteds = new String[] {"1 2 30 (12)", "1 2 30 8 9 (10)", "1 2 30 8 5 (11)"};
    
    assertArrayEquals(expecteds, actuals);
  }
  
  private String[] getPrintOut(String str) {
    TriePrinter.printAsSets(str);
    
    return out.toString().split("\n");
  }
}
