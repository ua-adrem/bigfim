package ua.util;

import java.util.ArrayList;
import java.util.List;

public class Tools {
	public static int[] toIntArray(List<Integer> list) {
		int[] intArray = new int[list.size()];
		int ix = 0;
		for (Integer i : list) {
			intArray[ix++] = i;
		}
		return intArray;
	}

	public static int[] intersect(int[] tids1, int[] tids2) {
		List<Integer> intersection = new ArrayList<Integer>();

		int ix1 = 0, ix2 = 0;
		while (ix1 != tids1.length && ix2 != tids2.length) {
			int i1 = tids1[ix1];
			int i2 = tids2[ix2];
			if (i1 == i2) {
				intersection.add(i1);
				ix1++;
				ix2++;
			} else if (i1 < i2) {
				ix1++;
			} else {
				ix2++;
			}
		}

		return toIntArray(intersection);
	}

	public static int[] setDifference(int[] tids1, int[] tids2) {
		List<Integer> difference = new ArrayList<Integer>();

		int ix1 = 0, ix2 = 0;
		while (ix1 != tids1.length && ix2 != tids2.length) {
			int i1 = tids1[ix1];
			int i2 = tids2[ix2];
			if (i1 == i2) {
				ix1++;
				ix2++;
			} else if (i1 < i2) {
				difference.add(tids1[ix1]);
				ix1++;
			} else {
				ix2++;
			}
		}
		for (; ix1 < tids1.length; ix1++) {
			difference.add(tids1[ix1]);
		}

		return toIntArray(difference);
	}
}
