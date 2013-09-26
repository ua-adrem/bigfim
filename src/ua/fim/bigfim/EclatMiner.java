package ua.fim.bigfim;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;

import ua.fim.bigfim.util.Item;
import ua.fim.bigfim.util.SetReporter;
import ua.util.Tools;

import com.google.common.primitives.Ints;

public class EclatMiner {

	public static class AscendingItemComparator implements Comparator<Item> {
		@Override
		public int compare(Item o1, Item o2) {
			return Ints.compare(o1.freq(), o2.freq());
		}
	}

	private SetReporter reporter;

	public void setSetReporter(SetReporter setReporter) {
		this.reporter = setReporter;
	}

	public class C {
		private final int[] prefix;
		private final List<Item> items;

		public C(int[] prefix, List<Item> queue) {
			this.prefix = prefix;
			this.items = queue;
		}
	}

	public void mineRec(int[] prefix, List<Item> items, int minSup) {
		declatRec(prefix, items, minSup, true);
	}

	private void declatRec(int[] prefix, List<Item> items, int minSup,
			boolean tidLists) {
		Iterator<Item> it1 = items.iterator();
		int[] newPrefix = Arrays.copyOf(prefix, prefix.length + 1);
		for (int i = 0; i < items.size(); i++) {
			Item item1 = it1.next();
			int support = item1.freq();
			newPrefix[newPrefix.length - 1] = item1.id;

			reporter.report(newPrefix, support);

			if (i < items.size() - 1) {
				List<Item> newItems = new ArrayList<Item>(items.size() - i);
				ListIterator<Item> it2 = items.listIterator(i + 1);
				while (it2.hasNext()) {
					Item item2 = it2.next();
					int[] condTids;
					int[] tids1 = item1.getTids();
					int[] tids2 = item2.getTids();
					if (tidLists) {
						condTids = Tools.setDifference(tids1, tids2);
					} else {
						condTids = Tools.setDifference(tids2, tids1);
					}

					int newSupport = support - condTids.length;
					if (newSupport >= minSup) {
						Item newItem = new Item(item2.id, newSupport, condTids);
						newItems.add(newItem);
					}
				}
				if (newItems.size() > 0) {
					declatRec(newPrefix, newItems, minSup, false);
				}
			}

			if (closureCheck(item1)) {
				break;
			}
		}
	}

	public void mine(int[] prefix, List<Item> items, int minSup) {
		Queue<C> queue = new LinkedList<C>();
		queue.add(new C(prefix, items));
		declatNonRec(queue, minSup);
	}

	private void declatNonRec(Queue<C> queue, int minSup) {
		boolean tidLists = true;
		while (!queue.isEmpty()) {
			C c = queue.poll();
			int[] prefix = c.prefix;
			List<Item> items = c.items;

			Iterator<Item> it1 = items.iterator();
			for (int i = 0; i < items.size(); i++) {
				Item item1 = it1.next();
				int support = item1.freq();
				int[] newPrefix = Arrays.copyOf(prefix, prefix.length + 1);
				newPrefix[newPrefix.length - 1] = item1.id;

				if (i < items.size() - 1) {
					List<Item> newItems = new ArrayList<Item>(items.size() - i);
					ListIterator<Item> it2 = items.listIterator(i + 1);
					while (it2.hasNext()) {
						Item item2 = it2.next();
						int[] condTids;
						int[] tids1 = item1.getTids();
						int[] tids2 = item2.getTids();
						if (tidLists) {
							condTids = Tools.setDifference(tids1, tids2);
						} else {
							condTids = Tools.setDifference(tids2, tids1);
						}

						int newSupport = support - condTids.length;
						if (newSupport >= minSup) {
							Item newItem = new Item(item2.id, newSupport,
									condTids);
							newItems.add(newItem);
						}
					}
					if (newItems.size() > 0) {
						queue.add(new C(newPrefix, newItems));
					}
				}
				reporter.report(newPrefix, support);

				if (closureCheck(item1)) {
					break;
				}
			}
			tidLists = false;
		}
	}

	private boolean closureCheck(Item item) {
		return item.getTids().length == 0;
	}

	public static void main(String[] args) throws NumberFormatException,
			IOException {
		String filename = "data/test1.txt";
		int minSup = 5;
		List<Item> items = readItemsFromFile(filename);
		EclatMiner miner = new EclatMiner();
		miner.setSetReporter(new SetReporter.CmdReporter());
		miner.mine(new int[] {}, items, minSup);
	}

	private static List<Item> readItemsFromFile(String filename)
			throws NumberFormatException, IOException {
		List<Item> items = new ArrayList<Item>();

		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line;
		while ((line = reader.readLine()) != null) {
			String[] split = line.split(" ");
			int[] tids = new int[split.length];
			for (int i = 1; i < split.length; i++) {
				tids[i] = Integer.parseInt(split[i]);
			}
			items.add(new Item(Integer.parseInt(split[0]), tids.length, tids));
		}
		reader.close();
		return items;
	}
}