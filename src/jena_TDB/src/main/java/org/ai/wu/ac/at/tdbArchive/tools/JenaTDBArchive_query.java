package org.ai.wu.ac.at.tdbArchive.tools;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ai.wu.ac.at.tdbArchive.api.JenaTDBArchive;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_CB;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_CBTB;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_Hybrid;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_IC;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_TB;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_TB_star;
import org.ai.wu.ac.at.tdbArchive.solutions.DiffSolution;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class JenaTDBArchive_query {

	public static void main(String[] args) throws FileNotFoundException, InterruptedException, ExecutionException {

		String dirTDBs = null;
		String queryFile = null;
		String queryFileDynamic = null;
		Boolean bulkQueries = false;
		String outputResults = "";
		String queryCategory = "all"; // query everything;
		String outputTime = "";
		String policy = "ic";
		Options options = new Options();
		int versionQuery = 0;
		int endversionQuery = 0;
		int jump = 0;
		String rol = "subject"; // for bulk queries
		Boolean silent = false;
		Boolean splitResultsByVersion=false;
		try {

			System.out.println(WelcomeASCII());
			/*
			 * We assume the following structure: - 0 .. 57 ---- add/ ---- del/
			 */

			Option queryPolicyOpt = new Option("p", "policy", true, "Policy implementation: ic | cb | tb | cbtb | hybrid ");
			queryPolicyOpt.setRequired(true);
			options.addOption(queryPolicyOpt);

			Option inputDirOpt = new Option("d", "dir", true, "DIR to load TDBs");
			inputDirOpt.setRequired(true);
			options.addOption(inputDirOpt);

			Option queryOpt = new Option("q", "query", true, "single SPARQL query to process, applied on -v version");
			queryOpt.setRequired(false);
			options.addOption(queryOpt);

			Option filequeryOpt = new Option("Q", "MultipleQueries", true, "file with several SPARQL queries");
			filequeryOpt.setRequired(false);
			options.addOption(filequeryOpt);

			Option fileDynqueryOpt = new Option("a", "allVersionQueries", true, "dynamic queries to process in all versions");
			fileDynqueryOpt.setRequired(false);
			options.addOption(fileDynqueryOpt);

			Option queryCatOpt = new Option("c", "category", true, "Query category: mat | diff | ver | change ");
			queryCatOpt.setRequired(false);
			options.addOption(queryCatOpt);

			Option versionOpt = new Option("v", "version", true, "Version, used in the Query (e.g. in materialize)");
			versionOpt.setRequired(false);
			options.addOption(versionOpt);
			Option postversionOpt = new Option("e", "endversion", true, "Version end, used in the Query (e.g. in diff)");
			postversionOpt.setRequired(false);
			options.addOption(postversionOpt);

			Option outputDirOpt = new Option("o", "OutputResults", true, "Output file with Results");
			outputDirOpt.setRequired(false);
			options.addOption(outputDirOpt);
			
			Option SplitVersionOpt = new Option("S", "SplitResults", false, "Split Results by version (creates one file fer version)");
			SplitVersionOpt.setRequired(false);
			options.addOption(SplitVersionOpt);

			Option rolOpt = new Option("r", "rol", true, "Rol of the Resource in the query: subject (s) | predicate (p) | object (o)");
			rolOpt.setRequired(false);
			options.addOption(rolOpt);

			Option timeOpt = new Option("t", "timeOutput", true, "file to write the time output");
			timeOpt.setRequired(false);
			options.addOption(timeOpt);

			Option jumpCatOpt = new Option("j", "jump", true, "Jump step for the diff: e.g. 5 (0-5,0-10..)");
			jumpCatOpt.setRequired(false);
			options.addOption(jumpCatOpt);

			Option silentOpt = new Option("s", "silent", false, "Silent output, that is, don't show results");
			silentOpt.setRequired(false);
			options.addOption(silentOpt);

			Option helpOpt = new Option("h", "help", false, "Shows help");
			helpOpt.setRequired(false);
			options.addOption(helpOpt);

			// Parse input arguments
			CommandLineParser cliParser = new BasicParser();
			CommandLine cmdLine = cliParser.parse(options, args);

			// Read arguments
			if (cmdLine.hasOption("p")) {
				policy = cmdLine.getOptionValue("p");
			}
			if (cmdLine.hasOption("d")) {
				dirTDBs = cmdLine.getOptionValue("d");
			}
			if (cmdLine.hasOption("c")) {
				queryCategory = cmdLine.getOptionValue("c");
			}
			if (cmdLine.hasOption("v")) {
				versionQuery = Integer.parseInt(cmdLine.getOptionValue("v"));
			}
			
			if (cmdLine.hasOption("e")) {
				endversionQuery = Integer.parseInt(cmdLine.getOptionValue("e"));
			}
			if (cmdLine.hasOption("o")) {
				outputResults = cmdLine.getOptionValue("o");
			}
			
			if (cmdLine.hasOption("t")) {
				outputTime = cmdLine.getOptionValue("t");
			}
			if (cmdLine.hasOption("q")) {
				queryFile = cmdLine.getOptionValue("q");
			}

			if (cmdLine.hasOption("Q")) {
				queryFile = cmdLine.getOptionValue("Q");
				bulkQueries = true;
			}
			if (cmdLine.hasOption("a")) {
				queryFileDynamic = cmdLine.getOptionValue("a");
				bulkQueries = true;
			}

			if (cmdLine.hasOption("h") || cmdLine.hasOption("help")) {
				HelpFormatter format = new HelpFormatter();
				format.printHelp("App", options);
			}
			if (cmdLine.hasOption("r")) {
				rol = cmdLine.getOptionValue("r");
			}
			if (cmdLine.hasOption("j")) {
				jump = Integer.parseInt(cmdLine.getOptionValue("j"));
			}
			if (cmdLine.hasOption("s")) {
				silent = true;
			}
			if (cmdLine.hasOption("S")) {
				splitResultsByVersion = true;
			}

		} catch (ParseException e) {
			HelpFormatter format = new HelpFormatter();
			format.printHelp("App", options);
			Logger.getLogger(JenaTDBArchive_query.class.getName()).log(Level.SEVERE, null, e);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Start Archive
		JenaTDBArchive jenaArchive = new JenaTDBArchive_IC();
		if (policy.equalsIgnoreCase("ic")) {
			jenaArchive = new JenaTDBArchive_IC();
		} else if (policy.equalsIgnoreCase("cb")) {
			jenaArchive = new JenaTDBArchive_CB();
		} else if (policy.equalsIgnoreCase("tb")) {
			jenaArchive = new JenaTDBArchive_TB();
		} else if (policy.equalsIgnoreCase("tb_star")) {
			jenaArchive = new JenaTDBArchive_TB_star();
		} else if (policy.equalsIgnoreCase("cbtb")) {
			jenaArchive = new JenaTDBArchive_CBTB();
		} else if (policy.equalsIgnoreCase("hybrid")) {
			jenaArchive = new JenaTDBArchive_Hybrid();
		}
		if (outputTime != "") {
			jenaArchive.setOutputTime(outputTime);
		}
		System.out.println("Loading archive "+policy.toUpperCase()+"...");
		long startTime = System.currentTimeMillis();
		jenaArchive.load(dirTDBs);
		long endTime = System.currentTimeMillis();
		System.out.println("Loaded in "+(endTime - startTime) +" ms");
		
		PrintStream os = System.out;
		if (outputResults != "") {
			if (!splitResultsByVersion)
				os = new PrintStream(outputResults);
		}
		PrintStream printStream = new PrintStream(os);

		try {
			System.out.println("* Your Query is " + queryCategory);
			if (queryCategory.equalsIgnoreCase("mat")) {

				/*
				 * ONE QUERY
				 */
				if (!bulkQueries) {
					String queryString = readFile(queryFile, StandardCharsets.UTF_8);
					System.out.println("** The input query is '" + queryCategory + "' at version " + versionQuery);
					ArrayList<String> solution = jenaArchive.matQuery(versionQuery, queryString);
					if (!silent) {
						os.println("\n**** SOLUTIONS:");
						printSolution(os, solution);
					}
				}
				/*
				 * FILE CONTAINS SEVERAL QUERIES
				 */
				else {

					if (queryFile != null) {
						ArrayList<ArrayList<String>> solution = jenaArchive.bulkMatQuerying(queryFile, rol);
						if (!silent) {
							os.println("\n**** SOLUTIONS bulkMatQuerying:");
							if (!splitResultsByVersion)
								printSolutionSeveralQueries(os, solution);
							else
								printSolutionSeveralQueries(outputResults, solution);
						}
					} else {
						ArrayList<Map<Integer, ArrayList<String>>> solution = jenaArchive.bulkAllMatQuerying(queryFileDynamic, rol);
						if (!silent) {
							os.println("\n**** SOLUTIONS bulkAllMatQuerying:");
							if (!splitResultsByVersion)
								printSolutionSeveralQueriesAllVersions(os, solution);
							else
								printSolutionSeveralQueriesAllVersions(outputResults,solution);
						}
					}
				}
			} else if (queryCategory.equalsIgnoreCase("diff")) {

				if (!bulkQueries) {
					System.out.println("Diff between version " + versionQuery + " and version " + endversionQuery);
					String queryString = readFile(queryFile, StandardCharsets.UTF_8);

					DiffSolution solution = jenaArchive.diffQuerying(versionQuery, endversionQuery, queryString);
					if (!silent) {
						os.println("\n**** SOLUTIONS:");
						printSolutionDiff(os, solution);
					}
				} else {
					System.out.println("Diff of all versions ");
					if (jump > 0)
						System.out.println("     with jump " + jump);
					ArrayList<Map<Integer, DiffSolution>> solution = jenaArchive.bulkAlldiffQuerying(queryFileDynamic, rol, jump);
					if (!silent) {
						os.println("\n**** SOLUTIONS:");
						if (!splitResultsByVersion)
							printSolutionDiffAll(os, solution);
						else
							printSolutionDiffAll(outputResults,solution);
					}
				}
			}

			else if (queryCategory.equalsIgnoreCase("ver")) {

				/*
				 * ONE QUERY
				 */
				if (!bulkQueries) {
					String queryString = readFile(queryFile, StandardCharsets.UTF_8);
					System.out.println(queryString);
					Map<Integer, ArrayList<String>> solution = jenaArchive.verQuery(queryString);
					if (!silent) {
						os.println("\n**** SOLUTIONS:");
						printSolutionVer(os, solution);
					}
				}
				/*
				 * FILE CONTAINS SEVERAL QUERIES
				 */
				else {
					ArrayList<Map<Integer, ArrayList<String>>> solution = jenaArchive.bulkAllVerQuerying(queryFileDynamic, rol);
					if (!silent) {
						os.println("\n**** SOLUTIONS:");
						if (!splitResultsByVersion)
							printSolutionVerSeveralQueries(os, solution);
						else
							printSolutionVerSeveralQueries(outputResults, solution);
					}
				}

			} else if (queryCategory.equalsIgnoreCase("change")) {
				System.out.println("Change between versions");

				// TODO TODO

			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		printStream.close();
	}

	private static void printSolutionDiffAll(PrintStream os, ArrayList<Map<Integer, DiffSolution>> sols) {

		for (Map<Integer, DiffSolution> solJump : sols) {
			for (Integer i : solJump.keySet()) {

				os.println("\nsolution jump " + i + ":");
				os.println("\nSolution ADDs:");
				for (String sol : solJump.get(i).getAdds()) {
					os.println(sol);
				}
				os.println("\nSolution DELs:");
				for (String sol : solJump.get(i).getDels()) {
					os.println(sol);
				}
			}
		}

	}
	private static void printSolutionDiffAll(String filename, ArrayList<Map<Integer, DiffSolution>> sols) throws FileNotFoundException {

		
		int numQuery=0;
		for (Map<Integer, DiffSolution> solJump : sols) {
			numQuery++;
			PrintStream os = new PrintStream(filename+"-query-"+numQuery);
			
			for (Integer i : solJump.keySet()) {
				
					
				for (String sol : solJump.get(i).getAdds()) {
					os.println("[ADD in jump "+i+"]" +sol);
				}
				for (String sol : solJump.get(i).getDels()) {
					os.println("[DEL in jump "+i+"]" +sol);
				}
				
			}
			os.close();
		}
		
	}

	private static void printSolutionDiff(PrintStream os, DiffSolution sols) {
		os.println("\nSolution ADDs:");
		for (String sol : sols.getAdds()) {
			os.println(sol);
		}
		os.println("\nSolution DELs:");
		for (String sol : sols.getDels()) {
			os.println(sol);
		}
	}

	private static void printSolution(PrintStream os, ArrayList<String> sols) {
		for (String sol : sols) {
			os.println(sol);
		}

	}

	private static void printSolutionSeveralQueries(PrintStream os, ArrayList<ArrayList<String>> sols) {
		int i = 1;
		for (ArrayList<String> solFile : sols) {
			os.println("\nsolution query " + i + ":");
			for (String sol : solFile) {
				os.println(sol);
			}
			i++;
		}
	}
	private static void printSolutionSeveralQueries(String filename, ArrayList<ArrayList<String>> sols) throws FileNotFoundException {
		int numQuery = 0;
		for (ArrayList<String> solFile : sols) {
			numQuery++;
			PrintStream os = new PrintStream(filename+"-query-"+numQuery);
			for (String sol : solFile) {
				os.println(sol);
			}

			os.close();
		}
	}

	private static void printSolutionSeveralQueriesAllVersions(PrintStream os, ArrayList<Map<Integer, ArrayList<String>>> sols) {
		int i = 1;
		for (Map<Integer, ArrayList<String>> solFile : sols) {
			os.println("\nsolution query " + i + ":");
			for (Integer version : solFile.keySet()) {
				os.println("\nsolution version " + version + ":");
				for (String sol : solFile.get(version)) {
					os.println(sol);
				}
			}
			i++;
		}

	}
	private static void printSolutionSeveralQueriesAllVersions(String filename, ArrayList<Map<Integer, ArrayList<String>>> sols) throws FileNotFoundException {
		int numQuery = 0;
		for (Map<Integer, ArrayList<String>> solFile : sols) {
			numQuery++;
			PrintStream os = new PrintStream(filename+"-query-"+numQuery);
			for (Integer version : solFile.keySet()) {
				for (String sol : solFile.get(version)) {
					os.println("[Solution in version "+version+"]"+sol);
				}
			}
			os.close();
		}

	}
	private static void printSolutionVer(PrintStream os, Map<Integer, ArrayList<String>> sols) {

		for (Integer i : sols.keySet()) {

			os.println("\nsolution version " + i + ":");

			for (String sol : sols.get(i)) {
				os.println(sol);
			}
		}
	}

	private static void printSolutionVerSeveralQueries(PrintStream os, ArrayList<Map<Integer, ArrayList<String>>> sols) {
		int queryno=1;
		for (Map<Integer, ArrayList<String>> solutionQueries : sols) {
			os.println("\nsolution query " + queryno + ":");
			for (Integer i : solutionQueries.keySet()) {

				os.println("\nsolution version " + i + ":");

				for (String sol : solutionQueries.get(i)) {
					os.println(sol);
				}
			}
			queryno++;

		}

	}
	private static void printSolutionVerSeveralQueries(String filename, ArrayList<Map<Integer, ArrayList<String>>> sols) throws FileNotFoundException {
		int numQuery=0;
		for (Map<Integer, ArrayList<String>> solutionQueries : sols) {
			numQuery++;
			PrintStream os = new PrintStream(filename+"-query-"+numQuery);
			for (Integer i : solutionQueries.keySet()) {

				for (String sol : solutionQueries.get(i)) {
					os.println("[Solution in version "+i+"]"+sol);
				}
			}
			os.close();
		}

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
