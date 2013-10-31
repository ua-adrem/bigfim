package org.apache.mahout.fpm.util;

import java.util.HashMap;
import java.util.Map;

public class Trie {
  public int id;
  public int support;
  public Map<Integer,Trie> children;
  
  public Trie(int id) {
    this.id = id;
    support = 0;
    children = new HashMap<Integer,Trie>();
  }
  
  public Trie getChild(int id) {
    Trie child = children.get(id);
    if (child == null) {
      child = new Trie(id);
      children.put(id, child);
    }
    return child;
  }
  
  public void incrementSupport() {
    this.support++;
  }
  
  @Override
  public String toString() {
    return "[" + id + "(" + support + "):" + children + "]";
  }
}