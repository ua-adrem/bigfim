package org.apache.mahout.fpm.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Tools {
  
  /**
   * Cleans the Hadoop file system by deleting the specified files if they exist.
   * 
   * @param files
   *          the files to delete
   */
  public static void cleanDirs(String... files) {
    System.out.println("[Cleaning]: Cleaning HDFS before running Eclat");
    Configuration conf = new Configuration();
    for (String filename : files) {
      System.out.println("[Cleaning]: Trying to delete " + filename);
      Path path = new Path(filename);
      try {
        FileSystem fs = path.getFileSystem(conf);
        if (fs.exists(path)) {
          if (fs.delete(path, true)) {
            System.out.println("[Cleaning]: Deleted " + filename);
          } else {
            System.out.println("[Cleaning]: Error while deleting " + filename);
          }
        } else {
          System.out.println("[Cleaning]: " + filename + " does not exist on HDFS");
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
}
