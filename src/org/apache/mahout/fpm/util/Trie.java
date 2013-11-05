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

import java.util.HashMap;
import java.util.Map;

public class Trie {
  public int id;
  public int support;
  public Map<Integer,Trie> children;
  
  public Trie(int id) {
    this.id = id;
    support = 0;
    children = new HashMap<Integer,Trie>();
  }
  
  public Trie getChild(int id) {
    Trie child = children.get(id);
    if (child == null) {
      child = new Trie(id);
      children.put(id, child);
    }
    return child;
  }
  
  public void incrementSupport() {
    this.support++;
  }
  
  @Override
  public String toString() {
    return "[" + id + "(" + support + "):" + children + "]";
  }
}