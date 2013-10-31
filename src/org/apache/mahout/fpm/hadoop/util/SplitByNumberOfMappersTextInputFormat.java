package org.apache.mahout.fpm.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;
import org.apache.mahout.fpm.util.Config;

/**
 * Input format that splits a file in a number of chunks given by Config.NUMBER_OF_MAPPERS_KEY.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class SplitByNumberOfMappersTextInputFormat extends FileInputFormat<LongWritable,Text> {
  
  @Override
  public RecordReader<LongWritable,Text> createRecordReader(InputSplit genericSplit, TaskAttemptContext context)
      throws IOException {
    context.setStatus(genericSplit.toString());
    return new LineRecordReader();
  }
  
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    int numberOfSplits = getNumberOfSplits(job);
    for (FileStatus status : listStatus(job)) {
      splits.addAll(getSplitsForFile(status, job.getConfiguration(), numberOfSplits));
    }
    return splits;
  }
  
  /**
   * Gets the different file splits for the data based on a given number of splits
   * 
   * @param status
   *          file status
   * @param conf
   *          hadoop configuration object
   * @param numberOfSplits
   *          number of splits to split the data in
   * @return list of file splits
   * @throws IOException
   *           thrown if the file does not exist
   */
  private static List<FileSplit> getSplitsForFile(FileStatus status, Configuration conf, int numberOfSplits)
      throws IOException {
    List<FileSplit> splits = new ArrayList<FileSplit>();
    Path fileName = status.getPath();
    if (status.isDir()) {
      throw new IOException("Not a file: " + fileName);
    }
    long totalNumberOfLines = getTotalNumberOfLines(conf, fileName);
    int numLinesPerSplit = (int) Math.ceil(1.0 * totalNumberOfLines / numberOfSplits);
    FileSystem fs = fileName.getFileSystem(conf);
    LineReader lr = null;
    try {
      FSDataInputStream in = fs.open(fileName);
      lr = new LineReader(in, conf);
      Text line = new Text();
      int numLines = 0;
      long begin = 0;
      long length = 0;
      int num = -1;
      while ((num = lr.readLine(line)) > 0) {
        numLines++;
        length += num;
        if (numLines == numLinesPerSplit) {
          splits.add(createFileSplit(fileName, begin, length));
          begin += length;
          length = 0;
          numLines = 0;
        }
      }
      if (numLines != 0) {
        splits.add(createFileSplit(fileName, begin, length));
      }
    } finally {
      if (lr != null) {
        lr.close();
      }
    }
    return splits;
  }
  
  /**
   * Gets the total number of lines from the file. If Config.NUMBER_OF_LINES_KEY is set, this value is returned.
   * 
   * @param conf
   *          hadoop configuration object
   * @param fileName
   *          name of file to count
   * @return the number of lines in the file
   * @throws IOException
   */
  private static long getTotalNumberOfLines(Configuration conf, Path fileName) throws IOException {
    long nrLines = conf.getLong(Config.NUMBER_OF_LINES_KEY, -1);
    if (nrLines != -1) {
      return nrLines;
    }
    
    FileSystem fs = fileName.getFileSystem(conf);
    LineReader lr = null;
    try {
      FSDataInputStream in = fs.open(fileName);
      lr = new LineReader(in, conf);
      Text text = new Text();
      nrLines = 0;
      while (lr.readLine(text) > 0) {
        nrLines++;
      }
      return nrLines;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return 0;
  }
  
  /**
   * Creates a new filesplit object
   * 
   * @param fileName
   *          name of the file to which filesplit corresponds
   * @param begin
   *          begin of the split
   * @param length
   *          length of the split
   * @return file split object
   */
  protected static FileSplit createFileSplit(Path fileName, long begin, long length) {
    return (begin == 0) ? new FileSplit(fileName, begin, length - 1, new String[] {}) : new FileSplit(fileName,
        begin - 1, length, new String[] {});
  }
  
  /**
   * Get the number of splits
   * 
   * @param job
   *          the job
   * @return the number of splits to be created on this file
   */
  public static int getNumberOfSplits(JobContext job) {
    return job.getConfiguration().getInt(Config.NUMBER_OF_MAPPERS_KEY, 1);
  }
}
