package ua.fim.eclat;

import static java.lang.Integer.parseInt;
import static java.util.Arrays.copyOf;
import static java.util.Collections.unmodifiableList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static ua.util.Tools.intersect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import ua.fim.eclat.util.Item;
import ua.fim.eclat.util.SetReporter;

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
  private List<Item> extensions;
  private Map<Integer,Item> itemMap;
  private int maxSize = -1;
  private int minSize = -1;

  // @formatter:on
  
  @Test
  public void finds_Frequent_Itemsets() {
    
    prepareData_5();
    
    extensions = items;
    final CollectReporter reporter = mineFor("", 10);
    
    final Object[][] expecteds = new Object[][] { {"0", 10}, {"2", 11}, {"3", 11}, {"3 5", 10}, {"4", 11}, {"5", 11},
        {"9", 10}};
    assertEqual(expecteds, reporter.itemsets);
  }
  
  @Test
  public void finds_Frequent_Itemsets_In_Conditional_Databases_1() {
    
    prepareData_5();
    
    final String prefix = "1";
    extensions = prepareExtensions(prefix, 2, 7);
    
    final CollectReporter reporter = mineFor(prefix, 6);
    
    final Object[][] expecteds = new Object[][] { {"1 2", 9}, {"1 7", 8}, {"1 2 7", 8}};
    assertEqual(expecteds, reporter.itemsets);
  }
  
  @Test
  public void finds_Frequent_Itemsets_In_Conditional_Databases_2() {
    
    prepareData_5();
    
    prepareExtensions("1", 2, 3, 5);
    
    final CollectReporter reporter = mineFor("1", 5);
    
    final Object[][] expecteds = new Object[][] { {"1 2", 9}, {"1 3", 5}, {"1 5", 5}, {"1 2 3", 5}, {"1 2 5", 5},
        {"1 3 5", 5}, {"1 2 3 5", 5}};
    assertEqual(expecteds, reporter.itemsets);
  }
  
  @Test
  public void only_The_Extensions_Are_Used() {
    
    prepareData_5();
    
    prepareExtensions("5", 8, 9);
    // Note that 3 and 9 are omitted from the extensions
    
    final CollectReporter reporter = mineFor("5", 5);
    
    final Object[][] expecteds = new Object[][] { {"5 8", 5}, {"5 9", 5}, {"5 8 9", 5}};
    assertEqual(expecteds, reporter.itemsets);
  }
  
  @Test
  public void prefix_Can_Be_Longer_Than_1_Item() {
    
    prepareData_5();
    
    prepareExtensions("5 8", 9, 0);
    final CollectReporter reporter = mineFor("5 8", 5);
    
    final Object[][] expecteds = new Object[][] { {"5 8 9", 5}, {"5 8 0", 5}, {"5 8 9 0", 5}};
    assertEqual(expecteds, reporter.itemsets);
  }
  
  @Test
  public void short_Itemsets_Are_Not_Reported() {
    prepareData_5();
    
    prepareExtensions("1", 2, 3, 5);
    // Maximal freq itemset for minSup=5 is "1 2 3 5"
    
    minSize = 3;
    final CollectReporter reporter = mineFor("1", 5);
    
    final Object[][] expecteds = new Object[][] { {"1 2 3", 5}, {"1 3 5", 5}, {"1 2 5", 5}, {"1 2 3 5", 5}};
    assertEqual(expecteds, reporter.itemsets);
  }
  
  @Test
  public void mine_The_Tree_Upto_A_Specified_Depth() {
    
    prepareData_5();
    
    prepareExtensions("1", 2, 3, 5);
    // Maximal freq itemset for minSup=5 is "1 2 3 5"
    
    maxSize = 2;
    final CollectReporter reporter = mineFor("1", 5);
    
    final Object[][] expecteds = new Object[][] { {"1 2", 9}, {"1 3", 5}, {"1 5", 5}};
    assertEqual(expecteds, reporter.itemsets);
  }
  
  @Test
  public void mine_Itemsets_With_Specific_Sizes() {
    
    prepareData_5();
    
    prepareExtensions("0", 3, 5, 8, 9);
    
    maxSize = 3;
    minSize = 3;
    final CollectReporter reporter = mineFor("0", 5);
    
    final Object[][] expecteds = new Object[][] { {"0 3 5", 5}, {"0 5 8", 5}, {"0 5 9", 5}, {"0 8 9", 9}};
    // {"0 3 8", 5}, {"0 3 9", 5} are not expected because of closedness
    assertEqual(expecteds, reporter.itemsets);
  }
  
  private List<Item> prepareExtensions(final String prefixStr, final int... ids) {
    int[] prefix = toIntArr(prefixStr);
    int[] prefixTids = itemMap.get(prefix[0]).getTids();
    
    for (int i = 1; i < prefix.length; i++) {
      prefixTids = intersect(prefixTids, itemMap.get(prefix[i]).getTids());
    }
    
    extensions = new ArrayList<Item>();
    for (int id : ids) {
      int[] tids = intersect(prefixTids, itemMap.get(id).getTids());
      extensions.add(new Item(id, tids.length, tids));
    }
    return extensions;
  }
  
  private void prepareData_5() {
    numOfItems = 10;
    createItems(data_5);
    mapItems();
  }
  
  private CollectReporter mineFor(final String prefixStr, final int minSup) {
    EclatMiner miner = new EclatMiner();
    int[] prefix = toIntArr(prefixStr);
    final CollectReporter reporter = new CollectReporter();
    miner.setSetReporter(reporter);
    
    if (maxSize > 0) {
      miner.setMaxSize(maxSize);
    }
    
    if (minSize > 0) {
      miner.setMinSize(minSize);
    }
    
    miner.mineRec(prefix, extensions, minSup);
    
//    System.out.println("for " + prefixStr + ", " + minSup + ", " + extensions);
//    for (Object[] is : reporter.itemsets) {
//      System.out.println(Arrays.toString((int[]) is[0]) + " " + is[1]);
//    }
    return reporter;
  }
  
  private static void assertEqual(final Object[][] expecteds, final List<Object[]> actualItemsets) {
    nextExpected: for (Object[] expected : expecteds) {
      for (Iterator<Object[]> it = actualItemsets.iterator(); it.hasNext();) {
        Object[] pair = it.next();
        
        int[] itemset = (int[]) pair[0];
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
    assertTrue("There are more itemsets than expected!", actualItemsets.isEmpty());
  }
  
  private static String itemsetToStr(int[] itemset) {
    String str = "";
    for (int item : itemset) {
      str += item + " ";
    }
    final String substring = str.substring(0, str.length() - 1);
    return substring;
  }
  
  private void mapItems() {
    itemMap = new HashMap<Integer,Item>();
    
    for (Item item : items) {
      itemMap.put(item.id, item);
    }
  }
  
  private void createItems(int[][] data) {
    List<ArrayList<Integer>> allTids = new ArrayList<ArrayList<Integer>>();
    
    for (int i = 0; i < numOfItems; i++) {
      allTids.add(new ArrayList<Integer>());
    }
    
    for (int i = 0; i < data.length; i++) {
      for (int j = 0; j < data[i].length; j++) {
        allTids.get(data[i][j]).add(i);
      }
    }
    
    items = new ArrayList<Item>(numOfItems);
    int itemIx = 0;
    for (ArrayList<Integer> tidList : allTids) {
      
      int[] tids = new int[tidList.size()];
      int tidIx = 0;
      for (int i : tidList) {
        tids[tidIx++] = i;
      }
      
      items.add(new Item(itemIx, tidList.size(), tids));
      itemIx++;
    }
  }
  
  private static int[] toIntArr(final String prefixStr) {
    if (prefixStr.length() == 0) {
      return new int[0];
    }
    String[] prefixArr = prefixStr.split(" ");
    int[] prefix = new int[prefixArr.length];
    for (int i = 0; i < prefixArr.length; i++) {
      prefix[i] = parseInt(prefixArr[i]);
    }
    return prefix;
  }
  
  private static class CollectReporter implements SetReporter {
    
    List<Object[]> itemsets;
    
    public CollectReporter() {
      itemsets = new ArrayList<Object[]>();
    }
    
    @Override
    public void close() {
      itemsets = unmodifiableList(itemsets);
    }
    
    @Override
    public void report(int[] itemset, int support, int[] tids) {
      final int[] itemset1 = copyOf(itemset, itemset.length);
      final int[] tids1 = copyOf(tids, tids.length);
      itemsets.add(new Object[] {itemset1, Integer.valueOf(support), tids1});
    }
  }
}
