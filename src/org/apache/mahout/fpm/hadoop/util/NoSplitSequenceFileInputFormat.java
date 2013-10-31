package org.apache.mahout.fpm.hadoop.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * Input format that never splits in file
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class NoSplitSequenceFileInputFormat extends SequenceFileInputFormat<Text,IntArrayWritable> {
  
  @Override
  public boolean isSplitable(JobContext jc, Path p) {
    return false;
  }
}