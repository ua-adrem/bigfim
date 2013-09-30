package ua.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

/**
 * Converts a database in transactional format to a database in vertical database format
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class DbTransposer {
  
  private static final String Delimiter = "\t";
  private static final String TidsFlag = "-tids";
  private static final String Extensions = ".dat";
  
  // items in the database and their tids
  private Map<String,BitSet> items;
  
  /**
   * Tranposes the input file to vertical database (item) format, and write it to file
   * 
   * 'input-extension'-tids.dat
   * 
   * @param inputFileName
   *          a database in transactional format
   * @throws FileNotFoundException
   */
  public void transpose(String inputFileName) throws FileNotFoundException {
    String outputFileName = inputFileName.substring(0, inputFileName.lastIndexOf('.')) + TidsFlag + Extensions;
    System.out.println("oFile " + outputFileName);
    if (new File(outputFileName).exists()) {
      System.out.println("File exists, aborting!");
      return;
    }
    
    items = new HashMap<String,BitSet>();
    readFile(inputFileName);
    writeItemsToFile(outputFileName);
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
    List<String> sortedItems = new ArrayList<String>(items.keySet());
    
    Collections.sort(sortedItems, new Comparator<String>() {
      @Override
      public int compare(String s1, String s2) {
        return new Integer(Integer.parseInt(s1)).compareTo(Integer.parseInt(s2));
      }
    });
    
    try {
      BufferedWriter w = new BufferedWriter(new FileWriter(outputFileName));
      
      for (String item : sortedItems) {
        StringBuffer buf = new StringBuffer();
        buf.append(item + Delimiter);
        BitSet bitSet = items.get(item);
        int ix = -1;
        while ((ix = bitSet.nextSetBit(ix + 1)) != -1) {
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
    int i = 0;
    while (scanner.hasNext()) {
      StringTokenizer tk = new StringTokenizer(scanner.nextLine());
      while (tk.hasMoreTokens()) {
        String item = tk.nextToken();
        BitSet tids = items.get(item);
        if (tids == null) {
          tids = new BitSet();
          items.put(item, tids);
        }
        tids.set(i);
      }
      i++;
    }
    scanner.close();
  }
  
  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Please specify: [inputFile]");
      return;
    }
    DbTransposer t = new DbTransposer();
    try {
      t.transpose(args[0]);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
