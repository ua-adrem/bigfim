package org.apache.mahout.fpm.disteclat;

import static org.apache.mahout.fpm.DriverTestHelper.delete;
import static org.apache.mahout.fpm.DriverTestHelper.readResults;

import java.io.File;

import org.apache.mahout.fpm.DriverTestHelper;
import org.junit.Before;
import org.junit.Test;

public class DistEclatDriverTest {
  private static final String Dist_Eclat_Output_File = "output/sample-60-3/fis/part-r-00000";
  private static final String Dist_Eclat_Config_File = "example/config-sample-Dist-Eclat.properties";
  private static boolean distEclatHasRun = false;
  private DriverTestHelper helper;
  
  @Before
  public void setUp() {
    helper = new DriverTestHelper();
  }
  
  @Test
  public void Dist_Eclat_Finds_Frequent_Itemsets() throws Exception {
    
    runDistEclatOnce();
    helper.assertAllOfThemFrequent(readResults(Dist_Eclat_Output_File));
  }
  
  @Test
  public void Dist_Eclat_Finds_All_The_Closed_Frequent_Itemsets() throws Exception {
    
    runDistEclatOnce();
    helper.assertAllFrequentsAreFound(readResults(Dist_Eclat_Output_File));
  }
  
  private static void runDistEclatOnce() {
    if (!distEclatHasRun) {
      try {
        delete(new File(Dist_Eclat_Output_File));
        DistEclatDriver.main(new String[] {Dist_Eclat_Config_File});
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    distEclatHasRun = true;
  }
}
