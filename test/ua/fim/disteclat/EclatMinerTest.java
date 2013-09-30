package ua.fim.disteclat;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import ua.fim.disteclat.util.Item;
import ua.fim.disteclat.util.SetReporter;

public class EclatMinerTest {
  
  // @formatter:off

  private static int[][] data_5 = new int[][] {
    { 0, 9, 3, 5, 8}, { 1, 4, 2, 6, 7},
    { 2, 4, 1, 7, 6}, { 3, 9, 0, 5, 8},
    { 4, 2, 1, 7, 6}, { 5, 8, 0, 9, 3},
    { 6, 1, 4, 2, 7}, { 7, 2, 4, 3, 9},
    { 8, 5, 0, 9, 3}, { 9, 3, 0, 5, 8},
    { 0, 6, 9, 4, 8}, { 1, 7, 3, 2, 5},
    { 2, 5, 3, 1, 7}, { 3, 1, 7, 2, 5},
    { 4, 5, 6, 0, 2}, { 5, 4, 2, 3, 1},
    { 6, 0, 9, 4, 8}, { 7, 1, 3, 2, 5},
    { 8, 9, 0, 6, 4}, { 9, 8, 0, 6, 4} };
  private int numOfItems;
  private List<Item> items;
  private Map<String,Integer> oneToOne;

  // @formatter:on
  
  @Test
  public void Finds_Frequent_Itemsets_In_Conditional_Databases_1() {
    
    prepareData_5();
    
    final CollectReporter reporter = mineFor("1", 6);
    
    final Object[][] expecteds = new Object[][] { {"1", 9}, {"1 2", 9}, {"1 2 7", 8}};
    assertEqual(expecteds, reporter.itemsets);
  }
  
  @Test
  public void Finds_Frequent_Itemsets_In_Conditional_Databases_2() {
    
    prepareData_5();
    
    final CollectReporter reporter = mineFor("5", 5);
    
    final Object[][] expecteds = new Object[][] { {"5", 11}, {"5 9", 5}, {"5 8 9", 5}};
    assertEqual(expecteds, reporter.itemsets);
  }
  
  private void prepareData_5() {
    numOfItems = 10;
    items = createItems(data_5, numOfItems);
    oneToOne = mapOneToOne(numOfItems);
  }
  
  private CollectReporter mineFor(final String prefix, final int minSup) {
    EclatMiner miner = new EclatMiner();
    final CollectReporter reporter = new CollectReporter();
    miner.setSetReporter(reporter);
    miner.mine(oneToOne, items, prefix, minSup);
    return reporter;
  }
  
  private static void assertEqual(final Object[][] expecteds, final List<Object[]> actualItemsets) {
    nextExpected: for (Object[] expected : expecteds) {
      for (Iterator it = actualItemsets.iterator(); it.hasNext();) {
        Object[] pair = (Object[]) it.next();
        
        List<Item> itemset = (List<Item>) pair[0];
        Integer support = (Integer) pair[1];
        
        if (support.equals(expected[1])) {
          String expectedItemset = (String) expected[0];
          if (expectedItemset.equals(itemsetToStr(itemset))) {
            it.remove();
            continue nextExpected;
          }
        }
      }
      fail("Expected itemset is not found:" + expected[0] + " (" + expected[1] + ") ");
    }
    
    assertTrue("There more itemsets then expected!", actualItemsets.isEmpty());
  }
  
  private static String itemsetToStr(List<Item> itemset) {
    String str = "";
    for (Item item : itemset) {
      str += item.name + " ";
    }
    final String substring = str.substring(0, str.length() - 1);
    return substring;
  }
  
  private static Map<String,Integer> mapOneToOne(final int numOfItems) {
    Map<String,Integer> oneToOne = new HashMap<String,Integer>();
    for (int i = 0; i < numOfItems; i++) {
      oneToOne.put(i + "", i);
    }
    return oneToOne;
  }
  
  private static List<Item> createItems(int[][] data, int numOfItems) {
    List<ArrayList<Integer>> allTids = new ArrayList<ArrayList<Integer>>();
    
    for (int i = 0; i < numOfItems; i++) {
      allTids.add(new ArrayList<Integer>());
    }
    
    for (int i = 0; i < data.length; i++) {
      for (int j = 0; j < data[i].length; j++) {
        allTids.get(data[i][j]).add(i);
      }
    }
    
    List<Item> items = new ArrayList<Item>(numOfItems);
    int itemIx = 0;
    for (ArrayList<Integer> tidList : allTids) {
      
      int[] tids = new int[tidList.size()];
      int tidIx = 0;
      for (int i : tidList) {
        tids[tidIx++] = i;
      }
      
      items.add(new Item(itemIx + "", tidList.size(), tids));
      itemIx++;
    }
    
    return items;
  }
  
  private static class CollectReporter implements SetReporter {
    
    List<Object[]> itemsets;
    
    public CollectReporter() {
      itemsets = new ArrayList<Object[]>();
    }
    
    @Override
    public void report(List<Item> itemset, int support) {
      itemsets.add(new Object[] {new ArrayList<Item>(itemset), Integer.valueOf(support)});
    }
    
    @Override
    public void report(List<Item> itemset, int support, int[] ints) {
      report(itemset, support);
    }
    
    @Override
    public void close() {}
  }
}
