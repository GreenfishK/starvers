package org.ai.wu.ac.at.rdfstarArchive.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class MergeResults {

	private static Options options;
	private static Integer noOfURIs;

	private static CommandLine parseOption(String[] args) {
		options = new Options();
		options.addOption("h", "help", false, "show help.");
		options.addOption("f", "format", true, "input format to split columns (csv|space)");
		options.addOption("1", true, "the first result file");
		options.addOption("2", true, "the second result file");
		options.addOption("t", true, "total queries");
		options.addOption("o", true, "output file");

		CommandLineParser parser = new BasicParser();
		try {
			CommandLine cmd = parser.parse(options, args);

			if (cmd.hasOption("h") || cmd.hasOption("help"))
				help();
			return cmd;
		} catch (ParseException e1) {
			System.err.println("Parsing failed.  Reason: " + e1.getMessage());
			help();
			return null;
		}
	}

	private static void help() {
		HelpFormatter formater = new HelpFormatter();
		formater.printHelp("Main", options);
		System.exit(0);
	}

	public static void main(String[] args) throws FileNotFoundException {
		// args = new String[]{"-i","empty/p/all.stats",
		// "-o","empty/p/",
		// // "-e","0.15",
		// "-s","10"};
		CommandLine cmd = parseOption(args);

		String input1 = cmd.getOptionValue("1");
		String input2 = cmd.getOptionValue("2");
		String output = cmd.getOptionValue("o");
		String format = cmd.getOptionValue("f");

		Integer numQueries = Integer.parseInt(cmd.getOptionValue("t"));

		Map<String, Double> totals = new HashMap<String, Double>();

		Boolean firstLine = true;
		String header = "";
		Scanner s = new Scanner(new File(input1));
		ArrayList<String> versions = new ArrayList<String>();
		
		while (s.hasNextLine()) {
			String line = s.nextLine();
			if (!firstLine) { // skip first comments
				if (line.trim().length() > 0) {
					String[] t={""};
					Double tot = 0.0;
					if (format.equalsIgnoreCase("csv")){
						t= line.trim().split(",");
						tot = Double.valueOf(t[2]);
					}
					else{
						t= line.trim().split(" ");
						tot = Double.valueOf(t[6]);
					}
					
					String version = t[0];
					totals.put(version, tot);
					versions.add(version);
			
				}
			} else {
				firstLine = false;
				header = line;
			}
		}
		s.close();
		s = new Scanner(new File(input2));
		firstLine = true;
		while (s.hasNextLine()) {
			String line = s.nextLine();
			if (!firstLine) { // skip first comments
				if (line.trim().length() > 0) {
					String[] t={""};
					Double tot = 0.0;
					if (format.equalsIgnoreCase("csv")){
						t= line.trim().split(",");
						tot = Double.valueOf(t[2]);
					}
					else{
						t= line.trim().split(" ");
						tot = Double.valueOf(t[6]);
					}
					String version = t[0];
					if (totals.get(version) != null) {
						totals.put(version, totals.get(version) + tot);
					} else {
						firstLine = false;
					}
				}
			} else {
				firstLine = false;
			}

		}
		s.close();
		File out = new File(output);

		PrintWriter pw = new PrintWriter(out);

		pw.println(header);
		String separator=",";
		if (format.equalsIgnoreCase("space")){
			separator=" ";
		}
		
		for (String i: versions){
			// print version,average,total
		
			pw.println(i + separator + (totals.get(i) / numQueries) + separator + totals.get(i));
		}
		pw.close();

	}

}