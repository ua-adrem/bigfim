package ua.fim.disteclat.util;

import static ua.fim.disteclat.util.PrefixGroupReporter.DELIMITER_EXTENSIONS;
import static ua.fim.disteclat.util.PrefixGroupReporter.DELIMITER_SUBEXTENSIONS;
import static ua.fim.disteclat.util.PrefixGroupReporter.DELIMITER_SUBEXTENSIONSLIST;
import static ua.fim.disteclat.util.PrefixGroupReporter.DELIMITER_SUPPORT;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import ua.fim.disteclat.util.PrefixGroupReporter.Extension;
import ua.hadoop.util.IntArrayWritable;
import ua.util.Tools;

public class Utils {

	public static List<Item> readTidLists(Configuration conf, Path path)
			throws IOException, URISyntaxException {
		SequenceFile.Reader r = new SequenceFile.Reader(FileSystem.get(new URI(
				"file:///"), conf), path, conf);

		List<Item> bitSets = new ArrayList<Item>();

		Text key = new Text();
		IntArrayWritable value = new IntArrayWritable();

		while (r.next(key, value)) {
			Writable[] tidListsW = value.get();

			int[] tids = new int[tidListsW.length];

			for (int i = 0; i < tidListsW.length; i++) {
				tids[i] = ((IntWritable) tidListsW[i]).get();
			}

			bitSets.add(new Item(key.toString(), tids.length, tids));
		}
		r.close();

		return bitSets;
	}

	public static Map<String, List<Extension>> readPrefixesGroups(
			Configuration conf, Path path) throws IOException,
			URISyntaxException {
		SequenceFile.Reader r = new SequenceFile.Reader(FileSystem.get(new URI(
				"file:///"), conf), path, conf);

		Map<String, List<Extension>> prefixGroups = new HashMap<String, List<Extension>>();

		Text key = new Text();
		Text value = new Text();

		while (r.next(key, value)) {
			String group = key.toString();
			String extsString = value.toString();
			String[] exts = extsString.split(DELIMITER_EXTENSIONS);
			List<Extension> extsList = new ArrayList<Extension>(exts.length);
			String last = exts[exts.length - 1];
			for (String extension : exts) {
				String[] ext = extension.split(DELIMITER_SUPPORT);
				Extension oExt = new Extension(ext[0]);
				if (extension == last) {
					String[] split = ext[1].split("\\"
							+ DELIMITER_SUBEXTENSIONSLIST);
					if (split.length == 2) {
						for (String subExtension : split[1].split("\\"
								+ DELIMITER_SUBEXTENSIONS)) {
							oExt.addSubExtension(subExtension);
						}
					}
				}
				extsList.add(oExt);
			}
			prefixGroups.put(group, extsList);
		}
		r.close();

		return prefixGroups;
	}

	public static String getGroupString(String itemsString) {
		StringBuilder builder = new StringBuilder();
		String[] items = itemsString.split("_");
		for (int i = 0; i < items.length - 1; i++) {
			builder.append(items[i] + "_");
		}
		return builder.toString();
	}

	public static Map<String, Integer> readSingletonsOrder(Configuration conf,
			Path path) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(
				path.toString()));

		String order = reader.readLine().trim();
		reader.close();

		Map<String, Integer> orderMap = new HashMap<String, Integer>();
		String[] split = order.split(" ");
		int ix = 0;
		for (String item : split) {
			orderMap.put(item, ix++);
		}
		return orderMap;
	}

	public static int[] intersect(Item item1, Item item2) {
		return Tools.intersect(item1.tids, item2.tids);
	}

	public static int[] setDifference(Item item1, Item item2) {
		return Tools.setDifference(item1.tids, item2.tids);
	}
}
