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

import static java.lang.Integer.parseInt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Converts a database in transactional format to a database in vertical database format
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class DbTransposer {
  
  private static final String Delimiter = "\t";
  private static final String TidsFlag = "-tids";
  
  // items in the database and their tids
  private Map<Long,BitSet> items;
  private int maxItems = Integer.MAX_VALUE;
  private Set<Long> writtenItems;
  
  /**
   * Transposes the input file to vertical database (item) format, and write it to file
   * 
   * 'input-extension'-tids.dat
   * 
   * @param inputFileName
   *          a database in horizontal format
   * @throws FileNotFoundException
   */
  public void transpose(String inputFileName) throws FileNotFoundException {
    
    int dotIndex = inputFileName.lastIndexOf('.');
    String outputFileName;
    if (dotIndex < 1) {
      outputFileName = inputFileName + TidsFlag;
    } else {
      outputFileName = inputFileName.substring(0, dotIndex) + TidsFlag + inputFileName.substring(dotIndex);
    }
    
    System.out.println("Output file: " + outputFileName);
    if (new File(outputFileName).exists()) {
      System.out.println("File exists, aborting!");
      return;
    }
    
    writtenItems = new HashSet<Long>();
    do {
      items = new HashMap<Long,BitSet>();
      readFile(inputFileName);
      writeItemsToFile(outputFileName);
      writtenItems.addAll(items.keySet());
    } while (items.size() == maxItems);
  }
  
  /**
   * Writes the individual items together with their tid lists to file as follows:
   * 
   * itemid tab tids
   * 
   * @param outputFileName
   *          name of file to write the vertical database to
   */
  private void writeItemsToFile(String outputFileName) {
    try {
      BufferedWriter w = new BufferedWriter(new FileWriter(outputFileName, true));
      
      for (Long item : items.keySet()) {
        StringBuffer buf = new StringBuffer();
        buf.append(item + Delimiter);
        BitSet set = items.get(item);
        int ix = -1;
        while ((ix = set.nextSetBit(ix + 1)) != -1) {
          buf.append(ix + " ");
        }
        w.write(buf.toString().trim());
        w.newLine();
      }
      w.flush();
      w.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Reads the input file in transactional database format
   * 
   * @param inputFileName
   *          name of the transaction database file
   * @throws FileNotFoundException
   */
  private void readFile(String inputFileName) throws FileNotFoundException {
    Scanner scanner = new Scanner(new File(inputFileName));
    int rowIx = 0;
    while (scanner.hasNext()) {
      StringTokenizer tk = new StringTokenizer(scanner.nextLine());
      while (tk.hasMoreTokens()) {
        Long item = Long.valueOf(tk.nextToken());
        BitSet tids = items.get(item);
        if (tids == null) {
          if (items.size() >= maxItems) {
            continue;
          }
          if (writtenItems.contains(item)) {
            continue;
          }
          tids = new BitSet();
          items.put(item, tids);
        }
        tids.set(rowIx);
      }
      rowIx++;
    }
    scanner.close();
  }
  
  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("Please specify: inputFile [items-per-iteration]");
      return;
    }
    
    DbTransposer t = new DbTransposer();
    
    if (args.length == 2) {
      t.maxItems = parseInt(args[1]);
    }
    
    try {
      t.transpose(args[0]);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
