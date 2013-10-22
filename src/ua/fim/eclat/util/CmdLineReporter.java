package ua.fim.eclat.util;

import java.util.Arrays;

public class CmdLineReporter implements SetReporter {
  
  @Override
  public void report(int[] itemset, int support, int[] tids) {
    System.out.println(Arrays.toString(itemset));
  }
  
  @Override
  public void close() {}
}