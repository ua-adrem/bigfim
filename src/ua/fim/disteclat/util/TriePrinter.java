package ua.fim.disteclat.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TriePrinter {
  public static final char SYMBOL = '$';
  public static final char SEPARATOR = '|';
  public static final char OPENSUP = '(';
  public static final char CLOSESUP = ')';
  
  public static void printAsSets(String trieString) {
    StringBuilder itemsetBuilder = new StringBuilder();
    StringBuilder supportBuilder = new StringBuilder();
    boolean readSupport = false;
    for (int i = 0; i < trieString.length(); i++) {
      char c = trieString.charAt(i);
      if (c == SYMBOL) {
        if (itemsetBuilder.length() == 0) {
          System.out.println("already 0");
        } else {
          itemsetBuilder.setLength(itemsetBuilder.length() - 1);
          int newLength = itemsetBuilder.lastIndexOf(" ") + 1;
          itemsetBuilder.setLength(newLength);
        }
      } else if (c == SEPARATOR) {
        itemsetBuilder.append(' ');
      } else if (c == OPENSUP) {
        readSupport = true;
        itemsetBuilder.append(' ');
      } else if (c == CLOSESUP) {
        System.out.print(itemsetBuilder.toString());
        System.out.println("(" + supportBuilder.toString() + ")");
        supportBuilder.setLength(0);
        readSupport = false;
      } else {
        if (readSupport) {
          supportBuilder.append(c);
        } else {
          itemsetBuilder.append(c);
        }
      }
    }
  }
  
  public static void main(String[] args) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader("mam-tids/fis/part-r-00000"));
    String line;
    while ((line = reader.readLine()) != null) {
      TriePrinter.printAsSets(line);
    }
    reader.close();
  }
}
