package org.apache.mahout.fpm.eclat.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class ItemTest {
  
  @Test
  public void testConstructor() {
    Item item = null;
    
    assertNull(item);
    
    item = new Item(1, 5, new int[] {0, 1, 2, 3, 4});
    assertNotNull(item);
    
    item = new Item(1, 2, new int[] {0, 1});
    assertNotNull(item);
  }
  
  @Test
  public void getFreq_Freq_TidsSize_Equal() {
    Item item = new Item(1, 5, new int[] {0, 1, 2, 3, 4});
    assertEquals(5, item.freq());
    
    item = new Item(1, 2, new int[] {0, 1});
    assertEquals(2, item.freq());
  }
  
  @Test
  public void getFreq_Freq_TidsSize_Not_Equal() {
    Item item = new Item(1, 5, new int[] {0, 1});
    assertEquals(5, item.freq());
    
    item = new Item(1, 2, new int[] {0, 1, 2, 3});
    assertEquals(2, item.freq());
  }
  
  @Test
  public void getTids_Freq_TidsSize_Equal() {
    Item item = new Item(1, 5, new int[] {0, 1, 2, 3, 4});
    assertArrayEquals(new int[] {0, 1, 2, 3, 4}, item.getTids());
    
    item = new Item(1, 2, new int[] {0, 1});
    assertArrayEquals(new int[] {0, 1}, item.getTids());
  }
  
  @Test
  public void getTids_Freq_TidsSize_Not_Equal() {
    Item item = new Item(1, 5, new int[] {0, 1});
    assertArrayEquals(new int[] {0, 1}, item.getTids());
    
    item = new Item(1, 2, new int[] {0, 1, 2, 3});
    assertArrayEquals(new int[] {0, 1, 2, 3}, item.getTids());
  }
}
