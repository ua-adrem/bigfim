package ua.fim.eclat.util;

public class CmdLineReporter implements SetReporter {
  
  @Override
  public void report(int[] itemset, int support) {
    StringBuilder builder = new StringBuilder();
    for (int item : itemset) {
      builder.append(item + " ");
    }
    builder.append("(" + support + ")");
    System.out.println(builder.toString());
  }
  
  @Override
  public void close() {}
}