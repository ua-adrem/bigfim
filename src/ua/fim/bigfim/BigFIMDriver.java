package ua.fim.bigfim;

import static java.io.File.separator;
import static ua.fim.configuration.Config.CLOSED_SETS_OPTIMIZATION_KEY;
import static ua.fim.configuration.Config.MAPRED_TASK_TIMEOUT_KEY;
import static ua.fim.configuration.Config.MIN_SUP_KEY;
import static ua.fim.configuration.Config.NUMBER_OF_LINES_KEY;
import static ua.fim.configuration.Config.NUMBER_OF_MAPPERS_KEY;
import static ua.fim.configuration.Config.PREFIX_LENGTH_KEY;
import static ua.fim.configuration.Config.WRITE_SETS_KEY;
import static ua.hadoop.util.Tools.cleanDirs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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

import ua.fim.configuration.Config;
import ua.fim.disteclat.DistEclatDriver;
import ua.hadoop.util.IntArrayWritable;
import ua.hadoop.util.NoSplitSequenceFileInputFormat;
import ua.hadoop.util.SplitByNumberOfMappersTextInputFormat;

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
		} else {
			config.readConfig(args[0]);
		}
		if (!config.isValid()) {
			System.out.println("Config file is invalid!");
			return -1;
		}
		config.printConfig();

		String iFile = config.getInputFile();
		String oFile = config.getOutputFile();
		cleanDirs(new String[] { oFile });
		long start = System.currentTimeMillis();
		long nrLines = startAprioriPhase(iFile, oFile, config);
		startCreatePrefixGroups(iFile, oFile, config, nrLines);
		startMining(iFile, oFile, config);
		long end = System.currentTimeMillis();

		System.out
				.println("[Eclat]: Total time: " + (end - start) / 1000 + "s");

		// getAvgNumberOfPrefixes(tmpDir2);
		// if (!config.getWriteSets()) {
		// getNumberOfItemsets(config.getOutputFile());
		// }
		return 0;
	}

	private static void getAvgNumberOfPrefixes(String dir) {
		Path path = new Path(dir + File.separator
				+ DistEclatDriver.OPrefixesDistribution + rExt);
		int total = 0;
		int nrOfLines = 0;
		try {
			FileSystem fs = FileSystem.get(new Configuration());

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			String line;
			while ((line = reader.readLine()) != null) {
				int tIx = line.indexOf('\t');
				String prefixes = line.substring(tIx + 1);
				StringTokenizer st = new StringTokenizer(prefixes);
				total += st.countTokens();
				nrOfLines++;
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("[Eclat Partition]: Average number of prefixes="
				+ (1.0 * total / nrOfLines));
	}

	private static void getNumberOfItemsets(String dir) {
		Path path = new Path(dir + File.separator + OFis + File.separator
				+ "part" + rExt);
		try {
			FileSystem fs = FileSystem.get(new Configuration());

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fs.open(path)));
			System.out.println("[Eclat Result]: Number of itemsets found");
			String line;
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void setConfigurationValues(Configuration conf, Config config) {
		conf.setInt(MIN_SUP_KEY, config.getMinSup());
		conf.setInt(NUMBER_OF_MAPPERS_KEY, config.getNumberOfMappers());
		conf.setInt(PREFIX_LENGTH_KEY, config.getPrefixLength());

		conf.setLong(MAPRED_TASK_TIMEOUT_KEY, config.getMapredTaskTimeout());

		conf.setBoolean(CLOSED_SETS_OPTIMIZATION_KEY,
				config.getClosedSetsOptimization());
		conf.setBoolean(WRITE_SETS_KEY, config.getWriteSets());
	}

	private static long startAprioriPhase(String inputFile, String outputFile,
			Config config) throws IOException, InterruptedException,
			ClassNotFoundException, URISyntaxException {
		long nrLines = -1;
		int prefixSize = config.getPrefixLength();
		for (int i = 1; i <= prefixSize; i++) {
			String outputDir = outputFile + separator + "ap" + i;
			String cacheFile = outputFile + separator + "ap" + (i - 1)
					+ separator + "part-r-00000";
			System.out.println("[AprioriPhase]: Phase: " + i + " input: "
					+ inputFile + ", output: " + outputFile);

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
				DistributedCache.addCacheFile(new URI(cacheFile),
						job.getConfiguration());
			}

			long start = System.currentTimeMillis();
			job.waitForCompletion(true);
			long end = System.currentTimeMillis();
			System.out.println("Job Apriori Phase " + i + " took "
					+ (end - start) / 1000 + "s");

			if (i == 1) {
				nrLines = job.getCounters()
						.findCounter(Task.Counter.MAP_INPUT_RECORDS).getValue();
			}
		}
		return nrLines;
	}

	private void startCreatePrefixGroups(String inputFile, String outputDir,
			Config config, long nrLines) throws IOException,
			ClassNotFoundException, InterruptedException, URISyntaxException {
		String cacheFile = outputDir + separator + "ap"
				+ config.getPrefixLength() + separator + "part-r-00000";
		String outputFile = outputDir + separator + "pg";
		System.out.println("[CreatePrefixGroups]: input: " + inputFile
				+ ", output: " + outputDir);

		Configuration conf = new Configuration();
		setConfigurationValues(conf, config);
		int subDbSize = (int) Math.ceil(1.0 * nrLines
				/ config.getNumberOfMappers());
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

		DistributedCache.addCacheFile(new URI(cacheFile),
				job.getConfiguration());

		long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		System.out.println("Job Prefix Creation took " + (end - start) / 1000
				+ "s");
	}

	private static void startMining(String inputFile, String outputDir,
			Config config) throws IOException, URISyntaxException,
			ClassNotFoundException, InterruptedException {
		String inputFilesDir = outputDir + separator + "pg" + separator;
		String outputFile = outputDir + separator + "fis";
		System.out.println("[StartMining]: input: " + inputFilesDir
				+ ", output: " + outputFile);

		Configuration conf = new Configuration();
		setConfigurationValues(conf, config);

		Job job = new Job(conf, "Start Mining");
		job.setJarByClass(BigFIMDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(EclatMinerSetCountMapper.class);
		job.setReducerClass(EclatMinerSetCountReducer.class);

		job.setInputFormatClass(NoSplitSequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(1);

		List<Path> inputPaths = new ArrayList<Path>();

		FileSystem fs = FileSystem.get(conf);
		FileStatus[] listStatus = fs.globStatus(new Path(inputFilesDir
				+ "bucket*"));
		for (FileStatus fstat : listStatus) {
			// if (fstat.getPath().toString().startsWith("bucket")) {
			inputPaths.add(fstat.getPath());
			// }
		}

		FileInputFormat.setInputPaths(job,
				inputPaths.toArray(new Path[inputPaths.size()]));
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