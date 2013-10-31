package org.apache.mahout.fpm.bigfim;

import static java.io.File.separator;
import static org.apache.mahout.fpm.hadoop.util.Tools.cleanDirs;
import static org.apache.mahout.fpm.util.Config.CLOSED_SETS_OPTIMIZATION_KEY;
import static org.apache.mahout.fpm.util.Config.MAPRED_TASK_TIMEOUT_KEY;
import static org.apache.mahout.fpm.util.Config.MIN_SUP_KEY;
import static org.apache.mahout.fpm.util.Config.NUMBER_OF_LINES_KEY;
import static org.apache.mahout.fpm.util.Config.NUMBER_OF_MAPPERS_KEY;
import static org.apache.mahout.fpm.util.Config.PREFIX_LENGTH_KEY;
import static org.apache.mahout.fpm.util.Config.WRITE_SETS_KEY;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.fpm.eclat.EclatMinerMapper;
import org.apache.mahout.fpm.eclat.EclatMinerMapperSetCount;
import org.apache.mahout.fpm.eclat.EclatMinerReducer;
import org.apache.mahout.fpm.eclat.EclatMinerReducerSetCount;
import org.apache.mahout.fpm.hadoop.util.IntArrayWritable;
import org.apache.mahout.fpm.hadoop.util.NoSplitSequenceFileInputFormat;
import org.apache.mahout.fpm.hadoop.util.SplitByNumberOfMappersTextInputFormat;
import org.apache.mahout.fpm.util.Config;

public class BigFIMDriver extends Configured implements Tool {
  
  private static final String OFis = "fis";
  public static final String rExt = "-r-00000";
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Please specify: [configuration file]");
      return -1;
    }
    for (String arg : args) {
      System.out.println(arg);
    }
    Config config = new Config();
    if (args[0].startsWith("s3n")) {
      Path path = new Path(args[0]);
      FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
      fs.open(path);
      config.readConfig(new InputStreamReader(fs.open(path)));
      fs.close();
    } else {
      config.readConfig(args[0]);
    }
    if (!config.isValid()) {
      System.out.println("Config file is invalid!");
      return -1;
    }
    
    config.printConfig();
    
    String inputDir = config.getInputFile();
    String outputDir = config.getOutputDir();
    cleanDirs(new String[] {outputDir});
    long start = System.currentTimeMillis();
    long nrLines = startAprioriPhase(inputDir, outputDir, config);
    startCreatePrefixGroups(inputDir, outputDir, config, nrLines);
    startMining(outputDir, config);
    long end = System.currentTimeMillis();
    
    System.out.println("[Eclat]: Total time: " + (end - start) / 1000 + "s");
    
    // getAvgNumberOfPrefixes(tmpDir2);
    // if (!config.getWriteSets()) {
    // getNumberOfItemsets(config.getOutputFile());
    // }
    return 0;
  }
  
  private static void setConfigurationValues(Configuration conf, Config config) {
    conf.setInt(MIN_SUP_KEY, config.getMinSup());
    conf.setInt(NUMBER_OF_MAPPERS_KEY, config.getNumberOfMappers());
    conf.setInt(PREFIX_LENGTH_KEY, config.getPrefixLength());
    
    conf.setLong(MAPRED_TASK_TIMEOUT_KEY, config.getMapredTaskTimeout());
    
    conf.setBoolean(CLOSED_SETS_OPTIMIZATION_KEY, config.getClosedSetsOptimization());
    conf.setBoolean(WRITE_SETS_KEY, config.getWriteSets());
  }
  
  private static long startAprioriPhase(String inputFile, String outputFile, Config config) throws IOException,
      InterruptedException, ClassNotFoundException, URISyntaxException {
    long nrLines = -1;
    int prefixSize = config.getPrefixLength();
    for (int i = 1; i <= prefixSize; i++) {
      String outputDir = outputFile + separator + "ap" + i;
      String cacheFile = outputFile + separator + "ap" + (i - 1) + separator + "part-r-00000";
      System.out.println("[AprioriPhase]: Phase: " + i + " input: " + inputFile + ", output: " + outputFile);
      
      Configuration conf = new Configuration();
      setConfigurationValues(conf, config);
      if (nrLines != -1) {
        conf.setLong(Config.NUMBER_OF_LINES_KEY, nrLines);
      }
      
      Job job = new Job(conf, "Apriori Phase" + i);
      job.setJarByClass(BigFIMDriver.class);
      
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      
      job.setMapperClass(AprioriPhaseMapper.class);
      job.setReducerClass(AprioriPhaseReducer.class);
      
      job.setInputFormatClass(SplitByNumberOfMappersTextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      job.setNumReduceTasks(1);
      
      FileInputFormat.addInputPath(job, new Path(inputFile));
      FileOutputFormat.setOutputPath(job, new Path(outputDir));
      
      if (i > 1) {
        DistributedCache.addCacheFile(new URI(cacheFile), job.getConfiguration());
      }
      
      long start = System.currentTimeMillis();
      job.waitForCompletion(true);
      long end = System.currentTimeMillis();
      System.out.println("Job Apriori Phase " + i + " took " + (end - start) / 1000 + "s");
      
      if (i == 1) {
        nrLines = job.getCounters().findCounter(Task.Counter.MAP_INPUT_RECORDS).getValue();
      }
    }
    return nrLines;
  }
  
  private static void startCreatePrefixGroups(String inputFile, String outputDir, Config config, long nrLines)
      throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    String cacheFile = outputDir + separator + "ap" + config.getPrefixLength() + separator + "part-r-00000";
    String outputFile = outputDir + separator + "pg";
    System.out.println("[CreatePrefixGroups]: input: " + inputFile + ", output: " + outputDir);
    
    Configuration conf = new Configuration();
    setConfigurationValues(conf, config);
    int subDbSize = (int) Math.ceil(1.0 * nrLines / config.getNumberOfMappers());
    conf.setLong(NUMBER_OF_LINES_KEY, nrLines);
    conf.setInt(Config.SUBDB_SIZE, subDbSize);
    
    Job job = new Job(conf, "Create Prefix Groups");
    job.setJarByClass(BigFIMDriver.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntArrayWritable.class);
    job.setOutputKeyClass(IntArrayWritable.class);
    job.setOutputValueClass(IntArrayWritable.class);
    
    job.setMapperClass(ComputeTidListMapper.class);
    job.setReducerClass(ComputeTidListReducer.class);
    
    job.setInputFormatClass(SplitByNumberOfMappersTextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    job.setNumReduceTasks(1);
    
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));
    
    DistributedCache.addCacheFile(new URI(cacheFile), job.getConfiguration());
    
    long start = System.currentTimeMillis();
    job.waitForCompletion(true);
    long end = System.currentTimeMillis();
    System.out.println("Job Prefix Creation took " + (end - start) / 1000 + "s");
  }
  
  private static void startMining(String outputDir, Config config) throws IOException, ClassNotFoundException,
      InterruptedException {
    String inputFilesDir = outputDir + separator + "pg" + separator;
    String outputFile = outputDir + separator + OFis;
    System.out.println("[StartMining]: input: " + inputFilesDir + ", output: " + outputFile);
    
    Configuration conf = new Configuration();
    setConfigurationValues(conf, config);
    
    Job job = new Job(conf, "Start Mining");
    job.setJarByClass(BigFIMDriver.class);
    
    job.setOutputKeyClass(Text.class);
    
    if (config.getWriteSets()) {
      job.setOutputValueClass(Text.class);
      job.setMapperClass(EclatMinerMapper.class);
      job.setReducerClass(EclatMinerReducer.class);
    } else {
      job.setOutputValueClass(LongWritable.class);
      job.setMapperClass(EclatMinerMapperSetCount.class);
      job.setReducerClass(EclatMinerReducerSetCount.class);
    }
    
    job.setInputFormatClass(NoSplitSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setNumReduceTasks(1);
    
    List<Path> inputPaths = new ArrayList<Path>();
    
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] listStatus = fs.globStatus(new Path(inputFilesDir + "bucket*"));
    fs.close();
    for (FileStatus fstat : listStatus) {
      inputPaths.add(fstat.getPath());
    }
    
    FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));
    
    long start = System.currentTimeMillis();
    job.waitForCompletion(true);
    long end = System.currentTimeMillis();
    System.out.println("Job Mining took " + (end - start) / 1000 + "s");
  }
  
  public static class PGPathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return path.toString().contains("-r-00");
    }
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BigFIMDriver(), args);
  }
  
}