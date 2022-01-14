package org.ai.wu.ac.at.tdbArchive.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;

import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_IC;
import org.ai.wu.ac.at.tdbArchive.utils.QueryUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;

public class AnalyseRes_OtherPatterns {

	public static void main(String[] args) throws ParseException, IOException, InterruptedException, ExecutionException {
		// TODO Auto-generated method stub
		Options options = new Options();
		Option dirOpt = new Option("d", true, "directory for tdb files");
		dirOpt.setRequired(true);
		options.addOption(dirOpt);
		Option file = new Option("f", true, "list of URLs");
		file.setRequired(true);
		options.addOption(file);
		Option output = new Option("o", true, "output directory");
		output.setRequired(true);
		options.addOption(output);
		Option values = new Option("t", true, "allowed values subject,predicate,object ");
		values.setRequired(true);
		options.addOption(values);
		Option versions = new Option("v", true, "number of versions");
		versions.setRequired(true);
		options.addOption(versions);
		Option limits = new Option("l", true, "limit the number of selected results");
		limits.setRequired(false);
		options.addOption(limits);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);

		String tdbFolder = cmd.getOptionValue("d");
		File resFile = new File(cmd.getOptionValue("f"));
		String type = cmd.getOptionValue("t");
		String numVersions = cmd.getOptionValue("v");
		int limit = Integer.MAX_VALUE;
		if (cmd.hasOption("l")){
			limit = Integer.parseInt(cmd.getOptionValue("l"));
			System.out.println("Sampling "+limit+" results");
		}
		File outDir = new File(cmd.getOptionValue("o"), type);

		if (!outDir.exists())
			outDir.mkdirs();

		long start = System.currentTimeMillis();
		JenaTDBArchive_IC jenaArchive = new JenaTDBArchive_IC();
		jenaArchive.load(tdbFolder);

		System.out.println("Loaded " + tdbFolder + " in " + (System.currentTimeMillis() - start) + " ms");

		InputStream is = new FileInputStream(resFile);
		if (resFile.getName().endsWith(".gz")) {
			is = new GZIPInputStream(is);
		}
		Scanner s = new Scanner(is);

		System.out.println("Read input file");
		while (s.hasNextLine()) {
			String line = s.nextLine();

			String[] uri = line.trim().split(" ");
			// System.out.println("uri: " + uri[0]);
			// We assume all the input is present in all versions
			// if(uri.length==2 && uri[0]==numVersions ){ //present in all versions (uri[0]==58)

			String query = QueryUtils.createLookupQuery(type, uri[0]);
			// System.out.println("query:"+query);
			Map<Integer, ArrayList<String>> solutions = new HashMap<Integer, ArrayList<String>>();
			ArrayList<String> element1Solutions = new ArrayList<String>();
			ArrayList<String> element2Solutions = new ArrayList<String>();

			HashSet<String> unrepeatedSolution = new HashSet<String>();

			for (int i = 0; i < Integer.parseInt(numVersions); i++) {
				ArrayList<String> solution = jenaArchive.matQuery(i, query);
				for (String sol : solution) {
					//System.out.println("Sol:"+sol);
					String firstElement =sol;
					String secondElement="";
					if (sol.indexOf(" ")!=-1){
						firstElement = sol.substring(0, sol.indexOf(" "));
						secondElement = sol.substring(sol.indexOf(" "));
					}
					// store only unrepeated solutions
					if (!unrepeatedSolution.contains(firstElement + " " + secondElement)) {
						// System.out.println("firstElement:" + firstElement + "!");
						element1Solutions.add(firstElement);
						// System.out.println("secondElement:" + secondElement + "!");
						element2Solutions.add(secondElement);

						unrepeatedSolution.add(firstElement + " " + secondElement);
					}
				}
				solutions.put(i, solution);
			}

			analyseQuery(solutions, outDir, uri[0], type);

			// Create stats for first pair, whether SP, PS, OS
			String newtype="",newtypeprint = "";
			String[] elements = { "", "",""};
		
			// System.out.println("newtype:" + newtype);
			int numQueries = 0;

			for (String elem : element1Solutions) {
				elem = elem.trim();
				if (numQueries < limit) {
					solutions = new HashMap<Integer, ArrayList<String>>();
					if (type.equalsIgnoreCase("s")) {
						newtype="sp";
						newtypeprint = "sp_from_s";
						elements[0] = uri[0];
						elements[1] = elem;
					} else if (type.equalsIgnoreCase("p")) {
						newtype="sp";
						newtypeprint = "sp_from_p";
						elements[1] = uri[0];
						elements[0] = elem;
					} else if (type.equalsIgnoreCase("o")) {
						newtype="so";
						newtypeprint = "so_from_o";
						elements[1] = uri[0];
						elements[0] = elem;
						if (!elements[1].startsWith("http") && !elements[1].startsWith("<")) {
							elements[1] = "\"" + elements[1] + "\"";
						}
					}

					query = QueryUtils.createLookupQuery(newtype, elements);
					for (int i = 0; i < Integer.parseInt(numVersions); i++) {
						//System.out.println("Query: "+query);
						ArrayList<String> solution = jenaArchive.matQuery(i, query);
						solutions.put(i, solution);
					}
					analyseQuery(solutions, outDir, elements[0] + " " + elements[1], newtypeprint);
				}
				numQueries++;
			}
			// Create stats for second pair, whether SO, PO, OP

			newtype = "";
		
			numQueries = 0;
			// System.out.println("newtype:" + newtype);
			for (String elem : element2Solutions) {
				elem = elem.trim();
				//System.out.println("elem:"+elem+"|");
				if (numQueries < limit) {
					solutions = new HashMap<Integer, ArrayList<String>>();
					if (type.equalsIgnoreCase("s")) {
						newtype="so";
						newtypeprint = "so_from_s";
						elements[0] = uri[0];
						elements[1] = elem;
						if (!elements[1].startsWith("http") && !elements[1].startsWith("<")) {
							elements[1] = "\"" + elements[1] + "\"";
						}
					} else if (type.equalsIgnoreCase("p")) {
						newtype="po";
						newtypeprint = "po_from_p";
						elements[0] = uri[0];
						elements[1] = elem;
						if (!elements[1].startsWith("http") && !elements[1].startsWith("<")) {
							elements[1] = "\"" + elements[1] + "\"";
						}
						//System.out.println("elements[1]:"+elements[1]+"|");
					} else if (type.equalsIgnoreCase("o")) {
						newtype="po";
						newtypeprint = "po_from_o";
						elements[1] = uri[0];
						elements[0] = elem;
						if (!elements[1].startsWith("http") && !elements[1].startsWith("<")) {
							elements[1] = "\"" + elements[1] + "\"";
						}
					}
					query = QueryUtils.createLookupQuery(newtype, elements);
					for (int i = 0; i < Integer.parseInt(numVersions); i++) {
						//System.out.println("Query: "+query);
						ArrayList<String> solution = jenaArchive.matQuery(i, query);
						solutions.put(i, solution);
					}
					analyseQuery(solutions, outDir, elements[0] + " " + elements[1], newtypeprint);
				}
				numQueries++;
			}

			// create stat for ask spo --> in this case we count how many solutions are positive because each version has always one solution
			// (positive or negative).
			// if we want all positives, get spo triples from static
			// otherwise, just random.
			
			for (int i = 0; i < Math.min(limit,element1Solutions.size()); i++) {
				String object = element2Solutions.get(i).trim();
				if (!object.startsWith("http") && !object.startsWith("<")) {
					object = "\"" + object + "\"";
					// System.out.println("object:"+object);
				}
				
				if (type.equalsIgnoreCase("s")) {
					elements[0] = uri[0];
					elements[1] = element1Solutions.get(i);
					elements[2] = object;
				} else if (type.equalsIgnoreCase("p")) {
					elements[1] = uri[0];
					elements[0] = element1Solutions.get(i);
					elements[2] = object;
				} else if (type.equalsIgnoreCase("o")) {
					elements[2] = uri[0];
					elements[0] = element1Solutions.get(i);
					elements[1] = object;
				}
				query = QueryUtils.createLookupQuery("spo", elements);
				Query queryJena = QueryFactory.create(query);
				// System.out.println("Query:" + query + "!");
				int numSolutions = 0;
				for (int j = 0; j < Integer.parseInt(numVersions); j++) {
					ArrayList<String> solution = jenaArchive.materializeASKQuery(j, queryJena);
					if (solution.get(0).equalsIgnoreCase("true")) {
						numSolutions++;
					}
				}
				FileWriter fw = new FileWriter(new File(outDir, "all_spo.stats"), true);
				fw.write(elements[0] + " " + elements[1] + " " + elements[2] + " " + numSolutions + "\n");
				fw.close();

				// analyseQuery(solutions, outDir, uri[0] + " " + element1Solutions.get(i) + " " + element2Solutions.get(i), "spo");
			}

		}
		s.close();
		jenaArchive.close();

		// QueryExecution qe = QueryExecutionFactory.create(query, dataset) ;
	}

	private static void analyseQuery(Map<Integer, ArrayList<String>> solutions, File outDir, String uri, String type) throws IOException {
		// System.out.println("analyse query " + type);

		DescriptiveStatistics _dynStats;
		DescriptiveStatistics _cardStats;

		_dynStats = new DescriptiveStatistics();
		_cardStats = new DescriptiveStatistics();

		_cardStats.addValue(solutions.get(0).size());
		TreeMap<Integer, Double> _dyn = new TreeMap<Integer, Double>();
		for (int i = 0; i < solutions.size() - 1; i++) {

			_cardStats.addValue(solutions.get(i + 1).size());

			ArrayList<String> a = solutions.get(i);
			ArrayList<String> b = solutions.get(i + 1);
			if (a.size() == 0) {
				System.out.println("WARN: empty results for snapshot " + i);
			}
			if (b.size() == 0) {
				System.out.println("WARN: empty results for snapshot " + (i + 1));
			}

			double dyn = 0;
			if (a.size() >= b.size()) {
				dyn = compare(b, a);
			} else {
				dyn = compare(a, b);
			}

			if (dyn == Double.NaN) {
				System.out.println("WARN: NAN for " + i + "-" + (i + 1));
			} else {
				_dyn.put(i, dyn);
			}

			// System.out.println("____________________");
			// if(dyn>0){
			// for(QuerySolution s: a){
			// System.out.println((i)+" "+s);
			//
			// }for(QuerySolution s: b){
			// System.out.println((i+1)+" "+s+ " "+a.contains(s));
			// }
			// }
			// System.out.println(dyn);
			_dynStats.addValue(dyn);
		}

		File dir = new File(outDir, "data_" + type);
		dir.mkdirs();
		// be careful with max length of filenames
		String filename = URLEncoder.encode(uri.replaceAll("<", "").replaceAll(">", ""));
		//get first caracters
		
		if (filename.length()>164){
			filename = filename.substring(0,164);
		}
		PrintWriter pw = new PrintWriter(new File(dir, filename + "_" + type + ".stats"));
		pw.println("1 " + solutions.get(1).size() + " 0.0");

		for (int i = 0; i < solutions.size() - 1; i++) {
			pw.println(i + " " + solutions.get(i).size() + " " + _dyn.get(i));
		}
		pw.close();
		FileWriter fw = new FileWriter(new File(outDir, "all_" + type + ".stats"), true);
		fw.write(uri + " " + _cardStats.getMin() + " " + _cardStats.getMean() + " " + _cardStats.getMax() + " " + _cardStats.getStandardDeviation()
				+ " " + _dynStats.getMin() + " " + _dynStats.getMean() + " " + _dynStats.getMax() + " " + _dynStats.getStandardDeviation() + "\n");
		fw.close();

	}

	private static double compare(ArrayList<String> b, ArrayList<String> a) {
		int total = b.size() + a.size();
		int same = 0;
		for (String sol : b) {
			if (a.contains(sol)) {
				same++;
			}
		}
		double d = (a.size() - same) + (b.size() - same);
		return (d / total);
	}

}