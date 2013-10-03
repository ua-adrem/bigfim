package ua.fim.disteclat;

import static java.io.File.separator;
import static org.apache.hadoop.mapreduce.lib.output.MultipleOutputs.addNamedOutput;
import static ua.fim.configuration.Config.CLOSED_SETS_OPTIMIZATION_KEY;
import static ua.fim.configuration.Config.MAPRED_TASK_TIMEOUT_KEY;
import static ua.fim.configuration.Config.MIN_SUP_KEY;
import static ua.fim.configuration.Config.NUMBER_OF_MAPPERS_KEY;
import static ua.fim.configuration.Config.PREFIX_LENGTH_KEY;
import static ua.fim.configuration.Config.WRITE_SETS_KEY;
import static ua.hadoop.util.Tools.cleanDirs;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ua.fim.configuration.Config;
import ua.fim.eclat.EclatMinerMapper;
import ua.fim.eclat.EclatMinerMapperSetCount;
import ua.fim.eclat.EclatMinerReducer;
import ua.fim.eclat.EclatMinerReducerSetCount;
import ua.hadoop.util.IntArrayWritable;
import ua.hadoop.util.NoSplitSequenceFileInputFormat;
import ua.hadoop.util.SplitByNumberOfMappersTextInputFormat;

//import org.apache.hadoop.io.LongWritable;

/**
 * Driver class for Dist-Eclat (distributed Eclat) implementation on the Hadoop framework. Dist-Eclat operates in three
 * steps and starts from databases in vertical format. It first mines X-FIs seed elements which it further distributes
 * among available mappers.
 * 
 * The first step consists of reading the vertical database file and reporting the frequent singletons. The latter are
 * distributed by the reducer into distinct groups. The distinct groups are used in the next cycle to compute X-FIs
 * seeds.The seeds are again distributed among a new batch of mappers. The mappers compute closed sets on their local
 * subtrees, indicated by the received prefixes.
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class DistEclatDriver extends Configured implements Tool {
  
  /*
   * ========================================================================
   * 
   * STATIC
   * 
   * ========================================================================
   */
  
  // output files first MapReduce cycle
  public static final String OSingletonsDistribution = "singletonsDistribution";
  public static final String OSingletonsOrder = "singletonsOrder";
  public static final String OSingletonsTids = "singletonsTids";
  
  // output files second MapReduce cycle
  public static final String OFises = "fises";
  public static final String OPrefixesDistribution = "prefixesDistribution";
  public static final String OPrefixesGroups = "prefixesGroups";
  
  // output files third MapReduce cycle
  private static final String OFis = "fis";
  
  // default extension for output file of first reducer
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
    
    String tmpDir1 = config.getOutputDir() + separator + "tmp1" + separator;
    String tmpDir2 = config.getOutputDir() + separator + "prefixes" + separator;
    
    long start = System.currentTimeMillis();
    cleanDirs(new String[] {config.getOutputDir(), tmpDir1, tmpDir2});
    startItemReading(config.getInputFile(), tmpDir1, config);
    startPrefixComputation(tmpDir1, tmpDir2, config);
    startMining(tmpDir2, config.getOutputDir(), config);
    long end = System.currentTimeMillis();
    
    System.out.println("[Eclat]: Total time: " + (end - start) / 1000 + "s");
    
    return 0;
  }
  
  /**
   * Passes all configuration flags to the Hadoop Configuration framework
   * 
   * @param conf
   *          the Hadoop configuration
   * @param config
   *          the configuration that has user-defined flags
   */
  private static void setConfigurationValues(Configuration conf, Config config) {
    conf.setInt(MIN_SUP_KEY, config.getMinSup());
    conf.setInt(NUMBER_OF_MAPPERS_KEY, config.getNumberOfMappers());
    conf.setInt(PREFIX_LENGTH_KEY, config.getPrefixLength());
    
    conf.setLong(MAPRED_TASK_TIMEOUT_KEY, config.getMapredTaskTimeout());
    
    conf.setBoolean(CLOSED_SETS_OPTIMIZATION_KEY, config.getClosedSetsOptimization());
    conf.setBoolean(WRITE_SETS_KEY, config.getWriteSets());
  }
  
  /**
   * Starts the first MapReduce cycle. First the file is partitioned into a number of chunks that is given to different
   * mappers. Each mapper reads the items together with their tid-list. It discards the infrequent ones and reports the
   * frequent ones. The reducer combines all frequent singletons, sorts them based on ascending frequency and divides
   * the singletons among available mappers.
   * 
   * This method generates three files, the frequent singletons (OSingletonsTids), the order file for singletons based
   * on ascending frequency (OSingletonsOrder) and the singletons distribution file (OSingletonsDistribution).
   * 
   * @param inputFile
   * @param outputFile
   * @param config
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws URISyntaxException
   */
  private static void startItemReading(String inputFile, String outputFile, Config config) throws IOException,
      InterruptedException, ClassNotFoundException, URISyntaxException {
    System.out.println("[ItemReading]: input: " + inputFile + ", output: " + outputFile);
    
    Configuration conf = new Configuration();
    setConfigurationValues(conf, config);
    
    Job job = new Job(conf, "Read Singletons");
    job.setJarByClass(DistEclatDriver.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntArrayWritable.class);
    
    job.setMapperClass(ItemReaderMapper.class);
    job.setReducerClass(ItemReaderReducer.class);
    
    job.setInputFormatClass(SplitByNumberOfMappersTextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setNumReduceTasks(1);
    
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));
    
    addNamedOutput(job, OSingletonsDistribution, TextOutputFormat.class, Text.class, Text.class);
    
    addNamedOutput(job, OSingletonsOrder, TextOutputFormat.class, Text.class, Text.class);
    
    addNamedOutput(job, OSingletonsTids, SequenceFileOutputFormat.class, Text.class, IntArrayWritable.class);
    
    long start = System.currentTimeMillis();
    job.waitForCompletion(true);
    long end = System.currentTimeMillis();
    System.out.println("Job Item Reading took " + (end - start) / 1000 + "s");
  }
  
  /**
   * Starts the second MapReduce cycle. Each mapper gets a list of singletons from which it should start building X-FIs.
   * Each mapper uses Eclat to quickly compute the list of X-FIs. The total set of X-FIs is again obtained by the
   * reducer, which then gets divided into independent sets. All sets that have been computed from level 1 to X are
   * already reported. The distribution of seeds is obtained by some allocation scheme, e.g., Round-Robin,
   * Lowest-Frequency, ...
   * 
   * This method generates three files, the frequent itemsets from level 1 to X (OFises), the prefix groups
   * (OPrefixGroups) and the prefix distribution file (OPrefixDistribution).
   * 
   * @param inputDir
   * @param outputDir
   * @param config
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws URISyntaxException
   */
  private static void startPrefixComputation(String inputDir, String outputDir, Config config) throws IOException,
      InterruptedException, ClassNotFoundException, URISyntaxException {
    
    String inputFile = inputDir + separator + OSingletonsDistribution + rExt;
    String outputFileFises = OFises;
    String outputFilePrefixes = OPrefixesDistribution;
    String singletonsOrderFile = inputDir + separator + OSingletonsOrder + rExt;
    String singletonsTidsFile = inputDir + separator + OSingletonsTids + rExt;
    
    System.out.println("[PrefixComputation]: input: " + inputFile + ", output fises: " + outputFileFises
        + ", output prefixes: " + outputFilePrefixes);
    
    Configuration conf = new Configuration();
    setConfigurationValues(conf, config);
    
    Job job = new Job(conf, "Compute Prefixes");
    job.setJarByClass(DistEclatDriver.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntArrayWritable.class);
    
    job.setMapperClass(PrefixComputerMapper.class);
    job.setReducerClass(PrefixComputerReducer.class);
    
    job.setInputFormatClass(NLineInputFormat.class);
    
    job.setNumReduceTasks(1);
    
    job.setOutputKeyClass(IntArrayWritable.class);
    job.setOutputValueClass(IntArrayWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    
    FileInputFormat.addInputPath(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputDir));
    
    DistributedCache.addCacheFile(new URI(singletonsOrderFile), job.getConfiguration());
    DistributedCache.addCacheFile(new URI(singletonsTidsFile), job.getConfiguration());
    
    long start = System.currentTimeMillis();
    job.waitForCompletion(true);
    long end = System.currentTimeMillis();
    System.out.println("[PartitionPrefixes]: Took " + (end - start) / 1000 + "s");
  }
  
  /**
   * Starts the third MapReduce cycle. Each mapper reads the prefix groups assigned to it and computes the collection of
   * closed sets. All information is reported to the reducer which finally writes the output to disk.
   * 
   * 
   * @param inputDir
   * @param outputDir
   * @param config
   * @throws IOException
   * @throws InterruptedException
   * @throws ClassNotFoundException
   * @throws URISyntaxException
   */
  private static void startMining(String inputDir, String outputDir, Config config) throws IOException,
      InterruptedException, ClassNotFoundException, URISyntaxException {
    
    String inputFilesDir = inputDir;
    String outputFile = outputDir + separator + OFis;
    System.out.println("[StartMining]: input: " + inputFilesDir + ", output: " + outputFile);
    
    Configuration conf = new Configuration();
    setConfigurationValues(conf, config);
    
    Job job = new Job(conf, "Start Mining");
    job.setJarByClass(DistEclatDriver.class);
    
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
    
    List<Path> inputPaths = new ArrayList<Path>();
    
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] listStatus = fs.globStatus(new Path(inputFilesDir + "bucket*"));
    for (FileStatus fstat : listStatus) {
      inputPaths.add(fstat.getPath());
    }
    
    FileInputFormat.setInputPaths(job, inputPaths.toArray(new Path[inputPaths.size()]));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));
    
    job.setOutputFormatClass(TextOutputFormat.class);
    
    job.setNumReduceTasks(1);
    
    long start = System.currentTimeMillis();
    job.waitForCompletion(true);
    long end = System.currentTimeMillis();
    System.out.println("[Mining]: Took " + (end - start) / 1000 + "s");
    fs.close();
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DistEclatDriver(), args);
  }
}