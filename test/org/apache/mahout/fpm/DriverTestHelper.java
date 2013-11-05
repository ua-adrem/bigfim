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
package org.apache.mahout.fpm;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.mahout.fpm.eclat.util.TriePrinter;

public class DriverTestHelper {
  
  static int[][] Expecteds = new int[][] { {12, 19, 18, 14}, {15, 19, 14, 18}, {15, 12, 14, 18}, {6, 19, 18, 14}};
  private List<Set<Integer>> expecteds;
  
  public DriverTestHelper() {
    expecteds = prepareExpecteds();
  }
  
  public void assertAllFrequentsAreFound(List<Set<Integer>> actuals) {
    nextExpected: for (Iterator<Set<Integer>> expIt = expecteds.iterator(); expIt.hasNext();) {
      Set<Integer> expected = expIt.next();
      
      for (Set<Integer> actual : actuals) {
        if (expected.equals(actual)) {
          expIt.remove();
          continue nextExpected;
        }
      }
    }
    
    if (!expecteds.isEmpty()) {
      fail("These should be frequent: " + expecteds);
    }
  }
  
  public void assertAllOfThemFrequent(List<Set<Integer>> actuals) {
    for (Set<Integer> expected : expecteds) {
      for (Iterator<Set<Integer>> it = actuals.iterator(); it.hasNext();) {
        Set<Integer> actual = it.next();
        
        if (expected.containsAll(actual)) {
          it.remove();
          continue;
        }
      }
    }
    
    if (!actuals.isEmpty()) {
      fail("These itemsets should NOT be frequent:" + actuals);
    }
  }
  
  public static List<Set<Integer>> readResults(final String outputFile) throws IOException, FileNotFoundException {
    File tempFile = File.createTempFile("fis", ".txt");
    tempFile.deleteOnExit();
    TriePrinter.main(new String[] {outputFile, tempFile.getAbsolutePath()});
    
    Scanner sc = new Scanner(tempFile);
    
    List<String> actualStrings = new ArrayList<String>(10);
    while (sc.hasNextLine()) {
      String itemsetStr = sc.nextLine().split("\t")[1];
      
      StringBuilder actualStr = new StringBuilder();
      
      int p2 = 0;
      int p1 = itemsetStr.indexOf('(');
      while (p1 >= 0) {
        actualStr.append(itemsetStr.substring(p2, p1)).append(" ");
        p2 = itemsetStr.indexOf(')', p1) + 1;
        p1 = itemsetStr.indexOf('(', p2);
      }
      
      actualStrings.add(actualStr.substring(0, actualStr.length() - 1));
    }
    sc.close();
    
    List<Set<Integer>> actuals = new ArrayList<Set<Integer>>(actualStrings.size());
    
    for (String actualString : actualStrings) {
      String[] actualArr = actualString.split(" ");
      Set<Integer> actual = new HashSet<Integer>();
      for (String i : actualArr) {
        actual.add(Integer.valueOf(i));
      }
      actuals.add(actual);
    }
    return actuals;
  }
  
  public static void delete(File file) {
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        delete(f);
      }
    }
    file.delete();
  }

  private static List<Set<Integer>> prepareExpecteds() {
    List<Set<Integer>> expectedsList = new ArrayList<Set<Integer>>(Expecteds.length);
    for (int[] expected : Expecteds) {
      Set<Integer> e = new HashSet<Integer>();
      for (int i : expected) {
        e.add(i);
      }
      expectedsList.add(e);
    }
    return expectedsList;
  }
}
