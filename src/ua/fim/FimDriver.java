package ua.fim;

import ua.fim.bigfim.BigFIMDriver;
import ua.fim.configuration.Config;
import ua.fim.disteclat.DistEclatDriver;

/**
 * Driver class for BigData algorithms and tools
 * 
 * @author Sandy Moens & Emin Aksehirli
 */
public class FimDriver {

	/*
	 * ========================================================================
	 * 
	 * STATIC
	 * 
	 * ========================================================================
	 */

	private static final String HELP = "HELP";

	public static enum FimVersion {
		DISTECLAT, BIGFIM
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			printHelp();
			return;
		}

		Config config = new Config();
		config.readConfig(args[0]);

		if (!config.isValid()) {
			System.out.println("Error in configuration file. Aborting!");
			return;
		}

		switch (config.getVersion()) {
		case DISTECLAT: {
			DistEclatDriver.main(new String[] { args[0] });
			break;
		}
		case BIGFIM: {
			BigFIMDriver.main(new String[] { args[0] });
			break;
		}
		default: {
			System.out.println("Invalid version specified!");
			printHelp();
		}
		}
	}

	private static void printHelp() {
		System.out.println("Please specify: [configFile]");
		System.out.println("For more information on config file please check example folder.");
	}
}
