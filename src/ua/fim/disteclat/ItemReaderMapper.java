package ua.fim.disteclat;

import static ua.fim.configuration.Config.MIN_SUP_KEY;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ua.hadoop.util.IntArrayWritable;

/**
 * This class implements the Mapper for the first MapReduce cycle for Dist-Eclat.It reads the database in vertical
 * format and reports only the frequent singletons. Each line in the database file should be formated as:
 * 'itemId<tab>comma-separated-tids'
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class ItemReaderMapper extends Mapper<LongWritable,Text,Text,IntArrayWritable> {
  
  /*
   * ========================================================================
   * 
   * STATIC
   * 
   * ========================================================================
   */
  
  public static final String tidsDelimiter = " ";
  public static final String tidsFileDelimiter = "\t";
  
  /*
   * ========================================================================
   * 
   * NON-STATIC
   * 
   * ========================================================================
   */
  
  private int minSup;
  
  @Override
  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    
    minSup = conf.getInt(MIN_SUP_KEY, 1);
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String[] split = value.toString().split(tidsFileDelimiter);
    String item = split[0];
    String[] tids = split[1].split(tidsDelimiter);
    
    if (tids.length < minSup) {
      return;
    }
    
    IntWritable[] iw = new IntWritable[tids.length];
    int ix = 0;
    for (String stringTid : tids) {
      iw[ix++] = new IntWritable(Integer.parseInt(stringTid));
    }
    
    context.write(new Text(item), new IntArrayWritable(iw));
  }
}