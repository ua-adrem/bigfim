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

import java.util.Arrays;

public class Item {
  
  public final int id;
  private final int support;
  protected final int[] tids;
  
  public Item(int id, int support, int[] tids) {
    this.id = id;
    this.support = support;
    this.tids = tids;
  }

  
  public int freq() {
    return support;
  }
  
  public int[] getTids() {
    return tids;
  }
  
  @Override
  public String toString() {
    return id + " (" + freq() + ")" + " [" + tids.length + "]";
  }


  @Override
  public int hashCode() {
    return id;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Item other = (Item) obj;
    if (id != other.id) return false;
    if (support != other.support) return false;
    if (!Arrays.equals(tids, other.tids)) return false;
    return true;
  }
  
}