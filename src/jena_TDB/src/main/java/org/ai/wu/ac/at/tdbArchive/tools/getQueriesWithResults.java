package org.ai.wu.ac.at.tdbArchive.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ai.wu.ac.at.tdbArchive.api.TripleStore;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_IC;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class getQueriesWithResults {

	public static void main(String[] args) throws FileNotFoundException, InterruptedException, ExecutionException {

		String dirTDBs = null;

		String queryFileDynamic = null;
		String outputResults = "";
		Options options = new Options();
		String rol = "subject"; // for bulk queries
		TripleStore tripleStore = null;

		try {

			System.out.println(WelcomeASCII());
			/*
			 * We assume the following structure: - 0 .. 57 ---- add/ ---- del/
			 */

			Option inputDirOpt = new Option("d", "dir", true, "DIR to load TDBs");
			inputDirOpt.setRequired(true);
			options.addOption(inputDirOpt);

			Option fileDynqueryOpt = new Option("a", "allVersionQueries", true, "dynamic queries to process in all versions");
			fileDynqueryOpt.setRequired(false);
			options.addOption(fileDynqueryOpt);

			Option outputDirOpt = new Option("o", "OutputResults", true, "Output file with Results");
			outputDirOpt.setRequired(false);
			options.addOption(outputDirOpt);

			Option rolOpt = new Option("r", "rol", true, "Rol of the Resource in the query: subject (s) | predicate (p) | object (o)");
			rolOpt.setRequired(false);
			options.addOption(rolOpt);

			Option helpOpt = new Option("h", "help", false, "Shows help");
			helpOpt.setRequired(false);
			options.addOption(helpOpt);

			Option tripleStoreOpt = new Option("T", "triplestore", false, "Triple store to load. e.g. Jena, GraphDB, ...");
			tripleStoreOpt.setRequired(false);
			options.addOption(tripleStoreOpt);

			// Parse input arguments
			CommandLineParser cliParser = new BasicParser();
			CommandLine cmdLine = cliParser.parse(options, args);

			// Read arguments

			if (cmdLine.hasOption("d")) {
				dirTDBs = cmdLine.getOptionValue("d");
			}

			if (cmdLine.hasOption("o")) {
				outputResults = cmdLine.getOptionValue("o");
			}

			if (cmdLine.hasOption("h") || cmdLine.hasOption("help")) {
				HelpFormatter format = new HelpFormatter();
				format.printHelp("App", options);
			}
			if (cmdLine.hasOption("r")) {
				rol = cmdLine.getOptionValue("r");
			}
			if (cmdLine.hasOption("a")) {
				queryFileDynamic = cmdLine.getOptionValue("a");
			}
			if (cmdLine.hasOption("T")) {
				switch(cmdLine.getOptionValue("T")) {
					case "JenaTDB": tripleStore = TripleStore.JenaTDB; break;
					case "GraphDB": tripleStore = TripleStore.GraphDB; break;
					default: throw new Exception("Please provide a valid triple store." +
							" Currently supported triple stores are JenaTDB and GraphDB.");
				}
			}

		} catch (ParseException e) {
			HelpFormatter format = new HelpFormatter();
			format.printHelp("App", options);
			Logger.getLogger(getQueriesWithResults.class.getName()).log(Level.SEVERE, null, e);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Start Archive
		JenaTDBArchive_IC jenaArchive = new JenaTDBArchive_IC();

		System.out.println("Loading archive IC...");
		long startTime = System.currentTimeMillis();
		jenaArchive.load(dirTDBs);
		long endTime = System.currentTimeMillis();
		System.out.println("Loaded in " + (endTime - startTime) + " ms");

		PrintStream os = System.out;
		if (outputResults != "") {
			os = new PrintStream(outputResults);
		}
		PrintStream printStream = new PrintStream(os);

		try {

			ArrayList<String> solution = jenaArchive.getQueriesWithResults(queryFileDynamic, rol);

			for (String sol : solution) {
				os.println(sol);
			}

			os.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		printStream.close();
	}

	static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	static String WelcomeASCII() {
		String ret = " .----------------.  .----------------.  .----------------.  .----------------.\n"
				+ "| .--------------. || .--------------. || .--------------. || .--------------. |\n"
				+ "| |   ______     | || |  _________   | || |      __      | || |  _______     | |\n"
				+ "| |  |_   _ \\    | || | |_   ___  |  | || |     /  \\     | || | |_   __ \\    | |\n"
				+ "| |    | |_) |   | || |   | |_  \\_|  | || |    / /\\ \\    | || |   | |__) |   | |\n"
				+ "| |    |  __'.   | || |   |  _|  _   | || |   / ____ \\   | || |   |  __ /    | |\n"
				+ "| |   _| |__) |  | || |  _| |___/ |  | || | _/ /    \\ \\_ | || |  _| |  \\ \\_  | |\n"
				+ "| |  |_______/   | || | |_________|  | || ||____|  |____|| || | |____| |___| | |\n"
				+ "| |              | || |              | || |              | || |              | |\n"
				+ "| '--------------' || '--------------' || '--------------' || '--------------' |\n"
				+ " '----------------'  '----------------'  '----------------'  '----------------'\n";
		return ret;
	}

}
