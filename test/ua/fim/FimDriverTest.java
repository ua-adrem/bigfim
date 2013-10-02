package ua.fim;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class FimDriverTest {
  
  private static final String Output_File = "output/sample-60-3/fis/part-r-00000";
  private static final String Config_File = "example/config-sample-Dist-Eclat.properties";
  
  static int[][] Expecteds = new int[][] { {12, 19, 18, 14}, {15, 19, 14, 18}, {15, 12, 14, 18}, {6, 19, 18, 14}};
  private List<Set<Integer>> expecteds;
  private List<Set<Integer>> actuals;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    runMiner();
  }
  
  @Before
  public void setUp() throws Exception {
    expecteds = prepareExpecteds();
    actuals = mineTheSampleAndReadResult();
  }
  
  @Test
  public void Dist_Eclat_Finds_Frequent_Itemsets() throws Exception {
    
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
  
  @Test
  public void Dist_Eclat_Finds_All_The_Closed_Frequent_Itemsets() throws Exception {
    
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
  
//  @Test
//  public void BigFim_Finds_Frequent_Itemsets() {
//    
//  }
  
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
  
  private static List<Set<Integer>> mineTheSampleAndReadResult() throws Exception, FileNotFoundException {
    
    Scanner sc = new Scanner(new File(Output_File));
    
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
  
  private static void runMiner() throws Exception {
    FimDriver.main(new String[] {Config_File});
  }
}
