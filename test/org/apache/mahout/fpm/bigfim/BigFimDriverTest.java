package org.apache.mahout.fpm.bigfim;

import static org.apache.mahout.fpm.DriverTestHelper.delete;
import static org.apache.mahout.fpm.DriverTestHelper.readResults;

import java.io.File;

import org.apache.mahout.fpm.DriverTestHelper;
import org.junit.Before;
import org.junit.Test;

public class BigFimDriverTest {
  private static boolean bigFimHasRun = false;
  private static final String BigFim_Output_File = "output/sample-bf-60-2/fis/part-r-00000";
  private static final String BigFim_Config_File = "example/config-sample-BigFim.properties";
  private DriverTestHelper helper;
  
  @Before
  public void setUp() {
    helper = new DriverTestHelper();
  }
  
  @Test
  public void BigFim_Finds_Frequent_Itemsets() throws Exception {
    
    runBigFimOnce();
    helper.assertAllOfThemFrequent(readResults(BigFim_Output_File));
  }
  
  @Test
  public void BigFim_Finds_All_The_Closed_Frequent_Itemsets() throws Exception {
    
    runBigFimOnce();
    helper.assertAllFrequentsAreFound(readResults(BigFim_Output_File));
  }
  
  private static void runBigFimOnce() {
    if (!bigFimHasRun) {
      try {
        delete(new File(BigFim_Output_File));
        BigFIMDriver.main(new String[] {BigFim_Config_File});
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    bigFimHasRun = true;
  }
}
