package org.apache.mahout.fpm.eclat.util;

public interface SetReporter {
  public void report(int[] itemset, int support);
  
  public void close();
}
