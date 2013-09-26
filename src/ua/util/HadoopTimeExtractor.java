package ua.util;

import static java.lang.Integer.parseInt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class HadoopTimeExtractor {

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("Please specify: (filename)+");
		}
		String line;
		for (String iFN : args) {
			int dotIx = iFN.lastIndexOf('.');
			String oFN = iFN.substring(0, dotIx) + "-timings.txt";

			BufferedReader r = new BufferedReader(new FileReader(iFN));
			BufferedWriter w = new BufferedWriter(new FileWriter(oFN));

			while ((line = r.readLine()) != null) {
				if (line.contains("sec")) {
					if (line.contains("hrs")) {
						parseHourString(line, w);
					} else if (line.contains("mins")) {
						parseMinString(line, w);
					} else {
						parseSecString(line, w);
					}
				}
			}
			r.close();
			w.close();
		}
	}

	private static void parseSecString(String line, BufferedWriter w)
			throws IOException {
		String[] split = line.split(" ");
		String sS = split[2];

		int sec = parseInt(sS.substring(1, sS.lastIndexOf('s')));

		int total = sec;
		w.write(" \t \t" + sec + "\t" + total);
		w.newLine();
	}

	private static void parseMinString(String line, BufferedWriter w)
			throws IOException {
		String[] split = line.split(" ");
		String mS = split[2];
		String sS = split[3];

		int min = parseInt(mS.substring(1, mS.lastIndexOf('m')));
		int sec = parseInt(sS.substring(0, sS.lastIndexOf('s')));

		int total = min * 60 + sec;
		w.write(" \t" + min + "\t" + sec + "\t" + total);
		w.newLine();
	}

	private static void parseHourString(String line, BufferedWriter w)
			throws IOException {
		String[] split = line.split(" ");
		String hS = split[2];
		String mS = split[3];
		String sS = split[4];

		int hou = parseInt(hS.substring(1, hS.lastIndexOf('h')));
		int min = parseInt(mS.substring(0, mS.lastIndexOf('m')));
		int sec = parseInt(sS.substring(0, sS.lastIndexOf('s')));

		int total = hou * 3600 + min * 60 + sec;
		w.write(hou + "\t" + min + "\t" + sec + "\t" + total);
		w.newLine();
	}
}
