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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;

import org.apache.mahout.fpm.eclat.util.CmdLineReporter;
import org.apache.mahout.fpm.eclat.util.Item;
import org.apache.mahout.fpm.eclat.util.SetReporter;
import org.apache.mahout.fpm.util.Tools;

public class EclatMiner {
  
  public static class AscendingItemComparator implements Comparator<Item> {
    @Override
    public int compare(Item o1, Item o2) {
      return Tools.compare(o1.freq(), o2.freq());
    }
  }
  
  private SetReporter reporter;
  private long maxSize = Long.MAX_VALUE;
  private int minSize;
  
  public void setSetReporter(SetReporter setReporter) {
    this.reporter = setReporter;
  }
  
  public class C {
    private final int[] prefix;
    private final List<Item> items;
    
    public C(int[] prefix, List<Item> queue) {
      this.prefix = prefix;
      this.items = queue;
    }
  }
  
  /**
   * Mines the sub prefix tree for frequent itemsets. items do not have to be conditioned, instead they should contain
   * full TID's.
   * 
   * @param item
   *          Prefix of the tree to mine.
   * @param items
   *          List of items with their initial TID lists.
   * @param minSup
   *          Minimum support
   */
  public void mineRecByPruning(Item item, List<Item> items, int minSup) {
    
    ArrayList<Item> newItems = new ArrayList<Item>();
    
    for (Iterator<Item> it = items.iterator(); it.hasNext();) {
      Item item_2 = it.next();
      if (item.id == item_2.id) {
        continue;
      }
      
      int[] condTids = Tools.setDifference(item.getTids(), item_2.getTids());
      
      int newSupport = item.freq() - condTids.length;
      if (newSupport >= minSup) {
        Item newItem = new Item(item_2.id, newSupport, condTids);
        newItems.add(newItem);
      }
    }
    if (newItems.size() > 0) {
      newItems.trimToSize();
      declatRec(new int[] {item.id}, newItems, minSup, false);
    }
  }
  
  /**
   * Mines the sub prefix tree for frequent itemsets.
   * 
   * @param prefix
   *          Prefix of the tree to mine.
   * @param items
   *          List of items with their conditional TID lists. All of the items should be frequent extensions of the
   *          prefix, i.e., support of union of prefix and each item should be greater than or equal to minSup.
   * @param minSup
   *          Minimum support
   */
  public void mineRec(int[] prefix, List<Item> items, int minSup) {
    declatRec(prefix, items, minSup, true);
  }
  
  private void declatRec(int[] prefix, List<Item> items, int minSup, boolean tidLists) {
    Iterator<Item> it1 = items.iterator();
    int[] newPrefix = Arrays.copyOf(prefix, prefix.length + 1);
    for (int i = 0; i < items.size(); i++) {
      Item item1 = it1.next();
      int support = item1.freq();
      newPrefix[newPrefix.length - 1] = item1.id;
      
      if (newPrefix.length >= minSize) {
        reporter.report(newPrefix, support);
      }
      
      if (newPrefix.length < maxSize && i < items.size() - 1) {
        List<Item> newItems = new ArrayList<Item>(items.size() - i);
        ListIterator<Item> it2 = items.listIterator(i + 1);
        while (it2.hasNext()) {
          Item item2 = it2.next();
          int[] condTids;
          int[] tids1 = item1.getTids();
          int[] tids2 = item2.getTids();
          if (tidLists) {
            condTids = Tools.setDifference(tids1, tids2);
          } else {
            condTids = Tools.setDifference(tids2, tids1);
          }
          
          int newSupport = support - condTids.length;
          if (newSupport >= minSup) {
            Item newItem = new Item(item2.id, newSupport, condTids);
            newItems.add(newItem);
          }
        }
        if (newItems.size() > 0) {
          declatRec(newPrefix, newItems, minSup, false);
        }
      }
      
      if (closureCheck(item1)) {
        break;
      }
    }
  }
  
  private void mine(int[] prefix, List<Item> items, int minSup) {
    Queue<C> queue = new LinkedList<C>();
    queue.add(new C(prefix, items));
    declatNonRec(queue, minSup);
  }
  
  private void declatNonRec(Queue<C> queue, int minSup) {
    boolean tidLists = true;
    while (!queue.isEmpty()) {
      C c = queue.poll();
      int[] prefix = c.prefix;
      List<Item> items = c.items;
      
      Iterator<Item> it1 = items.iterator();
      for (int i = 0; i < items.size(); i++) {
        Item item1 = it1.next();
        int support = item1.freq();
        int[] newPrefix = Arrays.copyOf(prefix, prefix.length + 1);
        newPrefix[newPrefix.length - 1] = item1.id;
        int[] tids1 = item1.getTids();
        
        if (i < items.size() - 1) {
          List<Item> newItems = new ArrayList<Item>(items.size() - i);
          ListIterator<Item> it2 = items.listIterator(i + 1);
          while (it2.hasNext()) {
            Item item2 = it2.next();
            int[] condTids;
            int[] tids2 = item2.getTids();
            if (tidLists) {
              condTids = Tools.setDifference(tids1, tids2);
            } else {
              condTids = Tools.setDifference(tids2, tids1);
            }
            
            int newSupport = support - condTids.length;
            if (newSupport >= minSup) {
              Item newItem = new Item(item2.id, newSupport, condTids);
              newItems.add(newItem);
            }
          }
          if (newItems.size() > 0) {
            queue.add(new C(newPrefix, newItems));
          }
        }
        reporter.report(newPrefix, support);
        
        if (closureCheck(item1)) {
          break;
        }
      }
      tidLists = false;
    }
  }
  
  private static boolean closureCheck(Item item) {
    return item.getTids().length == 0;
  }
  
  public static void main(String[] args) throws NumberFormatException, IOException {
    String filename = "data/test1.txt";
    int minSup = 5;
    List<Item> items = readItemsFromFile(filename);
    EclatMiner miner = new EclatMiner();
    miner.setSetReporter(new CmdLineReporter());
    miner.mine(new int[] {}, items, minSup);
  }
  
  private static List<Item> readItemsFromFile(String filename) throws NumberFormatException, IOException {
    List<Item> items = new ArrayList<Item>();
    
    BufferedReader reader = new BufferedReader(new FileReader(filename));
    String line;
    while ((line = reader.readLine()) != null) {
      String[] split = line.split(" ");
      int[] tids = new int[split.length];
      for (int i = 1; i < split.length; i++) {
        tids[i] = Integer.parseInt(split[i]);
      }
      items.add(new Item(Integer.parseInt(split[0]), tids.length, tids));
    }
    reader.close();
    return items;
  }
  
  public void setMaxSize(int maxSize) {
    this.maxSize = maxSize;
  }
  
  public void setMinSize(int minSize) {
    this.minSize = minSize;
  }
}