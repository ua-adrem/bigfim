package ua.fim.disteclat.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

public interface PrefixGroupReporter {
	public static final String PREFIX_GROUP = "PrefixGroup";
	public static final String DELIMITER_EXTENSIONS = " ";
	public static final String DELIMITER_SUBEXTENSIONSLIST = "|";
	public static final String DELIMITER_SUBEXTENSIONS = "$";
	public static final String DELIMITER_SUPPORT = "@";

	public static class Extension {
		private String name;
		private final List<String> subExtensions;
		private int support;

		public Extension() {
			subExtensions = new LinkedList<String>();
		}

		public Extension(String name) {
			this.name = name;
			subExtensions = new LinkedList<String>();
		}

		public void setSupport(int support) {
			this.support = support;
		}

		public void addSubExtension(String name) {
			this.subExtensions.add(name);
		}

		@Override
		public String toString() {
			if (subExtensions.isEmpty()) {
				return name + DELIMITER_SUPPORT + support;
			}
			StringBuilder builder = new StringBuilder();
			for (String subExtension : subExtensions) {
				builder.append(subExtension + DELIMITER_SUBEXTENSIONS);
			}
			return name + DELIMITER_SUPPORT + support
					+ DELIMITER_SUBEXTENSIONSLIST
					+ builder.substring(0, builder.length() - 1);
		}

		public String getName() {
			return name;
		}

		public List<String> getSubExtensions() {
			return subExtensions;
		}
	}

	public void report(String prefix, List<Extension> extensions);

	public void close();

	public static class NullReporter implements PrefixGroupReporter {

		@Override
		public void close() {
		}

		@Override
		public void report(String prefix, List<Extension> extensions) {
		}

	}

	public static class HadoopPGReporter implements PrefixGroupReporter {

		private final Context context;

		public HadoopPGReporter(Context context) {
			this.context = context;
		}

		@Override
		public void close() {
		}

		@Override
		public void report(String prefix, List<Extension> extensions) {
			try {
				StringBuilder builder = new StringBuilder();
				for (Extension extension : extensions) {
					builder.append(extension + " ");
				}
				context.write(
						new Text(PREFIX_GROUP + " " + prefix),
						new ObjectWritable(builder.substring(0,
								builder.length())));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}