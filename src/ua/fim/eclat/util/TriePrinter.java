package ua.fim.eclat.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;

public class TriePrinter {
  public static final char SYMBOL = '$';
  public static final char SEPARATOR = '|';
  public static final char OPENSUP = '(';
  public static final char CLOSESUP = ')';
  
  static PrintStream out = System.out;
  
  public static void printAsSets(String trieString) {
    StringBuilder itemsetBuilder = new StringBuilder();
    StringBuilder supportBuilder = new StringBuilder();
    boolean readSupport = false;
    for (int i = 0; i < trieString.length(); i++) {
      char c = trieString.charAt(i);
      if (c == SYMBOL) {
        if (itemsetBuilder.length() == 0) {
          out.println("already 0");
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
        out.print(itemsetBuilder.toString());
        out.println("(" + supportBuilder.toString() + ")");
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
    if (args.length == 0) {
      System.out.println("Usage: TriePrinter encoded-input-file [output-file]");
      System.out.println("\nIf the output file is not given, standart output will be used.");
      return;
    }
    
    if (args.length > 1) {
      TriePrinter.out = new PrintStream(new File(args[1]));
    }
    
    BufferedReader reader = new BufferedReader(new FileReader(args[0]));
    String line;
    while ((line = reader.readLine()) != null) {
      TriePrinter.printAsSets(line);
    }
    reader.close();
  }
}
