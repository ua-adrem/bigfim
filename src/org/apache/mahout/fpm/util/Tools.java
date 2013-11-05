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
package org.apache.mahout.fpm.util;

import java.util.ArrayList;
import java.util.List;

public class Tools {
  public static int[] toIntArray(List<Integer> list) {
    int[] intArray = new int[list.size()];
    int ix = 0;
    for (Integer i : list) {
      intArray[ix++] = i;
    }
    return intArray;
  }
  
  public static int[] intersect(int[] tids1, int[] tids2) {
    List<Integer> intersection = new ArrayList<Integer>();
    
    int ix1 = 0, ix2 = 0;
    while (ix1 != tids1.length && ix2 != tids2.length) {
      int i1 = tids1[ix1];
      int i2 = tids2[ix2];
      if (i1 == i2) {
        intersection.add(i1);
        ix1++;
        ix2++;
      } else if (i1 < i2) {
        ix1++;
      } else {
        ix2++;
      }
    }
    
    return toIntArray(intersection);
  }
  
  public static int[] setDifference(int[] tids1, int[] tids2) {
    List<Integer> difference = new ArrayList<Integer>();
    
    int ix1 = 0, ix2 = 0;
    while (ix1 != tids1.length && ix2 != tids2.length) {
      int i1 = tids1[ix1];
      int i2 = tids2[ix2];
      if (i1 == i2) {
        ix1++;
        ix2++;
      } else if (i1 < i2) {
        difference.add(tids1[ix1]);
        ix1++;
      } else {
        ix2++;
      }
    }
    for (; ix1 < tids1.length; ix1++) {
      difference.add(tids1[ix1]);
    }
    
    return toIntArray(difference);
  }
  
  /**
   * Copied directly from the JDK 7 to increase compatibility with Java 6.
   * 
   * Compares two {@code int} values numerically. The value returned is identical to what would be returned by:
   * 
   * <pre>
   * Integer.valueOf(x).compareTo(Integer.valueOf(y))
   * </pre>
   * 
   * @param x
   *          the first {@code int} to compare
   * @param y
   *          the second {@code int} to compare
   * @return the value {@code 0} if {@code x == y}; a value less than {@code 0} if {@code x < y}; and a value greater
   *         than {@code 0} if {@code x > y}
   * @since 1.7
   */
  public static int compare(int x, int y) {
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
  }
}
