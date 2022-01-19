package org.ai.wu.ac.at.tdbArchive.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import org.ai.wu.ac.at.tdbArchive.api.RDFArchive;
import org.ai.wu.ac.at.tdbArchive.api.TripleStore;
import org.ai.wu.ac.at.tdbArchive.solutions.DiffSolution;
import org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query;
import org.ai.wu.ac.at.tdbArchive.utils.QueryUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jena.query.*;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.util.FileManager;

public class JenaTDBArchive_CBTB implements RDFArchive {

	private int TOTALVERSIONS = 0;
	private static String prefixGraphsVersions = "http://example.org/version";

	private String outputTime = "timeApp.txt";
	private Dataset dataset;
	private Boolean measureTime = false;

	// True if versions are numbered from 0
	private Boolean versionsStartWith0 = true;

	public static String getPrefixGraphsVersions() {
		return prefixGraphsVersions;
	}

	public static void setPrefixGraphsVersions(String prefixGraphsVersions) {
		JenaTDBArchive_CBTB.prefixGraphsVersions = prefixGraphsVersions;
	}

	public Boolean getVersionsStartWith0() {
		return versionsStartWith0;
	}

	public void setVersionsStartWith0(Boolean versionsStartWith0) {
		this.versionsStartWith0 = versionsStartWith0;
	}

	/**
	 * @param outputTime
	 */
	public void setOutputTime(String outputTime) {
		this.outputTime = outputTime;
		this.measureTime = true;
	}

	public JenaTDBArchive_CBTB() {
		this.measureTime = false;
	}

	/**
	 * Load Jena TDB from directory
	 * 
	 * @param directory
	 */
	public void load(String directory) {
		// Initialize Jena
		ARQ.init();
		FileManager fm = FileManager.get();
		fm.addLocatorClassLoader(JenaTDBArchive_query.class.getClassLoader());
		dataset = TDBFactory.createDataset(directory);

		/*
		 * get number of graphs in order to know the number of versions
		 */

		String queryGraphs = QueryUtils.getGraphs();
		Query query = QueryFactory.create(queryGraphs);
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		ResultSet results = qexec.execSelect();
		while (results.hasNext()) {
			QuerySolution soln = results.next();
			String graphResponse = soln.getResource("graph").toString();
			String versionSuffix = graphResponse.split(prefixGraphsVersions)[1];
			int versionFull = Integer.parseInt(versionSuffix);
			int version = versionFull / 2;
			if (version > TOTALVERSIONS) {
				TOTALVERSIONS = version;
				if (versionsStartWith0) {
					TOTALVERSIONS += 1;
				}
			}
		}

	}

	/**
	 * Gets the result of the provided query in the provided version
	 * 
	 * @param version
	 * @param queryString
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public ArrayList<String> matQuery(int version, String queryString) throws InterruptedException, ExecutionException {
		Query query = QueryFactory.create(queryString);
		ArrayList<String> ret = materializeQuery(version, query);
		long startTime = System.currentTimeMillis();

		long endTime = System.currentTimeMillis();
		if (measureTime) {
			// System.out.println("Time:" + (endTime - startTime));
			PrintWriter pw;
			try {
				pw = new PrintWriter(new File(outputTime));
				pw.println((endTime - startTime));
				pw.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		return ret;
	}

	/**
	 * Gets the diff of the provided query between the two given versions
	 * 
	 * @param startVersionQuery
	 * @param endVersionQuery
	 * @param queryString
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public DiffSolution diffQuerying(int startVersionQuery, int endVersionQuery, String queryString) throws InterruptedException, ExecutionException {

		HashSet<String> finalAdds = new HashSet<String>();
		HashSet<String> finalDels = new HashSet<String>();

		Query query = QueryFactory.create(queryString);
		long startTime = System.currentTimeMillis();
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		try {
			ResultSet results = qexec.execSelect();

			Boolean higherVersion1 = false;
			Boolean higherVersion2 = false;

			Iterator<QuerySolution> sortResults = orderedResultSet(results, "graph");

			QuerySolution soln = null;
			while (sortResults.hasNext() && (!higherVersion1 | !higherVersion2)) {
				soln = sortResults.next();
				// assume we have a graph variable as a response
				String graphResponse = soln.getResource("graph").toString();
				String versionSuffix = graphResponse.split(prefixGraphsVersions)[1];
				int versionFull = Integer.parseInt(versionSuffix);

				int version = versionFull / 2;

				if (version > startVersionQuery) {
					higherVersion1 = true;
				}

				if (higherVersion1) {
					// System.out.println("going between both versions");
					// System.out.println("--version:" + version);
					Boolean isAdd = false; // true if is Deleted
					if (versionFull % 2 == 0) {
						isAdd = true;
					}

					if (version > endVersionQuery) {
						higherVersion2 = true;
					} else {

						// assume we have element1 and element2
						// variables as
						// a response
						String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);

						// System.out.println("****** RowResult: " + rowResult);

						if (isAdd) {
							// check if it was already as a delete
							// result and, if so, delete this
							if (!finalDels.remove(rowResult))
								finalAdds.add(rowResult);
						} else {
							// check if it was already as an added
							// result and, if so, delete this
							if (!finalAdds.remove(rowResult))
								finalDels.add(rowResult);
						}

					}
				}
			}

		} finally {
			long endTime = System.currentTimeMillis();
			if (measureTime) {
				PrintWriter pw;
				try {
					pw = new PrintWriter(new File(outputTime));
					pw.println((endTime - startTime));
					pw.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}

			}
			qexec.close();
		}
		return new DiffSolution(finalAdds, finalDels);
	}

	/**
	 * Reads input file with a Resource, and gets the diff result of the lookup of the provided Resource with the provided rol (Subject, Predicate,
	 * Object) for all versions between 0 and consecutive jumps
	 * 
	 * @param queryFile
	 * @param rol
	 * @param jump
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	public ArrayList<Map<Integer, DiffSolution>> bulkAlldiffQuerying(String queryFile, String rol, int jump) throws InterruptedException,
			ExecutionException, IOException {
		ArrayList<Map<Integer, DiffSolution>> ret = new ArrayList<Map<Integer, DiffSolution>>();

		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}

		Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;

		DescriptiveStatistics total = new DescriptiveStatistics();

		while ((line = br.readLine()) != null) {
			Map<Integer, DiffSolution> solutions = new HashMap<Integer, DiffSolution>();

			String[] parts = line.split(" ");
			// String element = parts[0];

			/*
			 * warmup the system
			 */
			warmup();

			int start = 0;
			int end = TOTALVERSIONS - 1;
			if (jump > 0) {
				end = ((TOTALVERSIONS - 1) / jump) + 1; // +1 to do one loop at
														// least
			}
			for (int index = start; index < end; index++) {
				// ArrayList<String> finalAdds = new ArrayList<String>();
				// ArrayList<String> finalDels = new ArrayList<String>();
				int versionQuery = index;
				int postversionQuery = versionQuery + 1;
				if (jump > 0) {
					postversionQuery = Math.min((index + 1) * jump, TOTALVERSIONS - 1);
					versionQuery = 0;
				}
				// System.out.println("versionQuery:" + versionQuery + " ; postQuery:" + postversionQuery);

				String queryString = QueryUtils.createLookupQueryGraph(rol, parts);
				// System.out.println("\n\n\nqueryString:" + queryString);
				Query query = QueryFactory.create(queryString);
				long startTime = System.currentTimeMillis();
				QueryExecution qexec = QueryExecutionFactory.create(query, dataset);

				ResultSet results = qexec.execSelect();

				Boolean higherVersion1 = false;
				Boolean higherVersion2 = false;
				HashSet<String> finalResultsAdd = new HashSet<String>();
				HashSet<String> finalResultsDel = new HashSet<String>();

				Iterator<QuerySolution> sortResults = orderedResultSet(results, "graph");

				QuerySolution soln = null;
				while (sortResults.hasNext() && (!higherVersion1 | !higherVersion2)) {
					soln = sortResults.next();
					// assume we have a graph variable as a response
					String graphResponse = soln.getResource("graph").toString();
                    if (!graphResponse.contains(prefixGraphsVersions)) break;
					String versionSuffix = graphResponse.split(prefixGraphsVersions)[1];
					int versionFull = Integer.parseInt(versionSuffix);
					// System.out.println("versionFull:"+versionFull);

					int version = versionFull / 2;

					if (version > versionQuery) {
						higherVersion1 = true;
					}

					if (higherVersion1) {
						// System.out.println("going between both versions");
						// System.out.println("--version:" + version);
						Boolean isAdd = false; // true if is Deleted
						if (versionFull % 2 == 0) {
							isAdd = true;
						}

						if (version > postversionQuery) {
							higherVersion2 = true;
						} else {

							// assume we have element1 and element2
							// variables as
							// a response
							String rowResult = "", checkOpposite = "";
							if (!askQuery) {
								rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);

								if (isAdd) {
									// check if it was already as a delete
									// result and, if so, delete this
									if (!finalResultsDel.remove(rowResult))
										finalResultsAdd.add(rowResult);
								} else {
									// check if it was already as an added
									// result and, if so, delete this
									if (!finalResultsAdd.remove(rowResult))
										finalResultsDel.add(rowResult);
								}
							} else {
								rowResult = "true";
								if (isAdd) {
									// check if it was already as a delete
									// result and, if so, delete this
									if (!finalResultsDel.remove(checkOpposite)) {
										finalResultsAdd.add(rowResult);
										finalResultsDel.add("true");
									}
								} else {
									// check if it was already as an added
									// result and, if so, delete this
									if (!finalResultsAdd.remove(checkOpposite)) {
										finalResultsDel.add(rowResult);
										finalResultsAdd.add("true");
									}
								}

							}

						}
					}
				}

				solutions.put(postversionQuery, new DiffSolution(finalResultsAdd, finalResultsDel));
				long endTime = System.currentTimeMillis();
				// System.out.println("Time:" + (endTime - startTime));
				total.addValue((endTime - startTime));
				vStats.get(index).addValue((endTime - startTime));
				qexec.close();

			}
			ret.add(solutions);
		}
		if (measureTime) {

			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("##bucket, min, mean, max, stddev, count,total");
			for (Entry<Integer, DescriptiveStatistics> ent : vStats.entrySet()) {
				pw.println(ent.getKey() + " " + ent.getValue().getMin() + " " + ent.getValue().getMean() + " " + ent.getValue().getMax() + " "
						+ ent.getValue().getStandardDeviation() + " " + ent.getValue().getN() + " " + ent.getValue().getSum());
			}
			pw.println("tot," + total.getMin() + "," + total.getMean() + "," + total.getMax() + "," + total.getStandardDeviation() + ","
					+ total.getN());
			pw.close();
		}
		br.close();
		return ret;
	}

	/**
	 * Reads input file with a Resource and a Version, and gets the result of a lookup of the provided Resource with the provided rol (Subject,
	 * Predicate, Object) in such Version
	 * 
	 * @param queryFile
	 * @param rol
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public ArrayList<ArrayList<String>> bulkMatQuerying(String queryFile, String rol) throws FileNotFoundException, IOException,
			InterruptedException, ExecutionException {
		ArrayList<ArrayList<String>> ret = new ArrayList<ArrayList<String>>();

		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";
		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}

		while ((line = br.readLine()) != null) {
			String[] parts = line.split(",");
			int staticVersionQuery = Integer.parseInt(parts[0]);
			String element = parts[3];
			String queryString = QueryUtils.createLookupQueryGraph(rol, element);

			// System.out.println("Query at version " + staticVersionQuery);
			// System.out.println(queryString);

			Query query = QueryFactory.create(queryString);
			long startTime = System.currentTimeMillis();

			// TOTALVERSIONS
			ret.add(materializeQuery(staticVersionQuery, query));

			long endTime = System.currentTimeMillis();
			// System.out.println("Time:" + (endTime - startTime));

			vStats.get(staticVersionQuery).addValue((endTime - startTime));

		}
		br.close();

		if (measureTime) {
			// PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-statmat-" + inputFile.getName()));
			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("##ver, min, mean, max, stddev, count");
			for (Entry<Integer, DescriptiveStatistics> ent : vStats.entrySet()) {
				pw.println(ent.getKey() + " " + ent.getValue().getMin() + " " + ent.getValue().getMean() + " " + ent.getValue().getMax() + " "
						+ ent.getValue().getStandardDeviation() + " " + ent.getValue().getN());
			}
			pw.close();
		}
		return ret;
	}

	/**
	 * Reads input file with a Resource, and gets the result of a lookup of the provided Resource with the provided rol (Subject, Predicate, Object)
	 * for every version
	 * 
	 * @param queryFile
	 * @param rol
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public ArrayList<Map<Integer, ArrayList<String>>> bulkAllMatQuerying(String queryFile, String rol) throws FileNotFoundException, IOException,
			InterruptedException, ExecutionException {
		ArrayList<Map<Integer, ArrayList<String>>> ret = new ArrayList<Map<Integer, ArrayList<String>>>();
		int[] timeVersion = new int[TOTALVERSIONS + 1];
		int[] numQueriesVersion = new int[TOTALVERSIONS + 1];
		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}
		Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;
		while ((line = br.readLine()) != null) {

			String[] parts = line.split(" ");

			// String element = parts[0];

			String queryString = QueryUtils.createLookupQueryGraph(rol, parts);

			// System.out.println("query: " + queryString);
			Query query = QueryFactory.create(queryString);

			/*
			 * warmup the system
			 */
			warmup();
			Map<Integer, ArrayList<String>> solutions = new HashMap<Integer, ArrayList<String>>();
			// System.out.println("TOTALVERSIONS:" + TOTALVERSIONS);
			for (int i = 0; i < TOTALVERSIONS; i++) {
				// System.out.println("Query at version " + i);
				long startTime = System.currentTimeMillis();
				if (!askQuery)
					solutions.put(i, materializeQuery(i, query));
				else
					solutions.put(i, materializeASKQuery(i, query));
				long endTime = System.currentTimeMillis();
				// System.out.println("Time:" + (endTime - startTime));
				vStats.get(i).addValue((endTime - startTime));
				timeVersion[i] += (endTime - startTime);
				numQueriesVersion[i] += 1;
			}
			ret.add(solutions);
		}
		br.close();

		if (measureTime)
			QueryUtils.logQueryStatistics(TripleStore.JenaTDB, outputTime, vStats);
		return ret;
	}

	/**
	 * @param staticVersionQuery
	 * @param query
	 * @return
	 */
	private ArrayList<String> materializeQuery(int staticVersionQuery, Query query) throws InterruptedException, ExecutionException {

		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);

		ResultSet results = qexec.execSelect();

		Boolean higherVersion = false;
		HashSet<String> finalResults = new HashSet<String>();

		Iterator<QuerySolution> sortResults = orderedResultSet(results, "graph");

		while (sortResults.hasNext() && !higherVersion) {

			QuerySolution soln = sortResults.next();
			// System.out.println(soln);
			// assume we have a graph variable as a response
			String graphResponse = soln.getResource("graph").toString();
			// System.out.println("--graphResponse:" + graphResponse);
            if (!graphResponse.contains(prefixGraphsVersions)) break;
			String versionSuffix = graphResponse.split(prefixGraphsVersions)[1];
			// System.out.println("--versionSuffix:" + versionSuffix);
			int versionFull = Integer.parseInt(versionSuffix);
			// System.out.println("--VersionFull:" + versionFull);
			int version = versionFull / 2;

			// System.out.println("--version:" + version);
			Boolean isAdd = false; // true if is Deleted
			if (versionFull % 2 == 0) {
				isAdd = true;
			}

			if (version > staticVersionQuery) {
				higherVersion = true;
			} else {
				// assume we have element1 and element2 variables as
				// a response
				String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);

				// System.out.println("****** RowResult: " + rowResult);
				if (isAdd) {
					finalResults.add(rowResult);
					// System.out.println("ADDED");
				} else {
					finalResults.remove(rowResult);
					// System.out.println("DEL");
				}
			}
		}
		qexec.close();
		return new ArrayList<String>(finalResults);
	}

	/**
	 * @param staticVersionQuery
	 * @param query
	 * @return
	 */
	private ArrayList<String> materializeASKQuery(int staticVersionQuery, Query query) throws InterruptedException, ExecutionException {

		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);

		ResultSet results = qexec.execSelect();

		Boolean higherVersion = false;
		ArrayList<String> finalResults = new ArrayList<String>();
		Boolean result = false;

		Iterator<QuerySolution> sortResults = orderedResultSet(results, "graph");

		while (sortResults.hasNext() && !higherVersion) {

			QuerySolution soln = sortResults.next();
			// System.out.println(soln);
			// assume we have a graph variable as a response
			String graphResponse = soln.getResource("graph").toString();
            if (!graphResponse.contains(prefixGraphsVersions)) break;
			// System.out.println("--graphResponse:" + graphResponse);
			String versionSuffix = graphResponse.split(prefixGraphsVersions)[1];
			// System.out.println("--versionSuffix:" + versionSuffix);
			int versionFull = Integer.parseInt(versionSuffix);
			// System.out.println("--VersionFull:" + versionFull);
			int version = versionFull / 2;

			// System.out.println("--version:" + version);
			Boolean isAdd = false; // true if is Deleted
			if (versionFull % 2 == 0) {
				isAdd = true;
			}

			if (version > staticVersionQuery) {
				higherVersion = true;
			} else {
				// assume we have element1 and element2 variables as
				// a response

				// System.out.println("****** RowResult: " + rowResult);
				if (isAdd) {
					result = true;
					// System.out.println("ADDED");
				} else {
					result = false;
					// System.out.println("DEL");
				}
			}
		}
		finalResults.add(result.toString());
		qexec.close();
		return finalResults;
	}

	private static Iterator<QuerySolution> orderedResultSet(ResultSet resultSet, final String sortingVariableName) {
		List<QuerySolution> list = new ArrayList<QuerySolution>();

		while (resultSet.hasNext()) {
			list.add(resultSet.nextSolution());
		}

		Collections.sort(list, new Comparator<QuerySolution>() {

			public int compare(QuerySolution a, QuerySolution b) {

				return a.getResource(sortingVariableName).toString().compareTo(b.getResource(sortingVariableName).toString());

			}
		});
		return list.iterator();
	}

	/**
	 * Get the results of the provided query in all versions
	 * 
	 * @param queryString
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public Map<Integer, ArrayList<String>> verQuery(String queryString) throws InterruptedException, ExecutionException {
		Map<Integer, ArrayList<String>> ret = new HashMap<Integer, ArrayList<String>>();
		Query query = QueryFactory.create(queryString);
		long startTime = System.currentTimeMillis();
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		try {
			ResultSet results = qexec.execSelect();

			HashSet<String> finalResults = new HashSet<String>();

			Iterator<QuerySolution> sortResults = orderedResultSet(results, "graph");
			int numRows = 0;
			int prevVersion = -1;
			while (sortResults.hasNext()) {
				numRows++;
				QuerySolution soln = sortResults.next();
				// assume we have a graph variable as a response
				String graphResponse = soln.getResource("graph").toString();
				// System.out.println("--graphResponse:" + graphResponse);
				String versionSuffix = graphResponse.split(prefixGraphsVersions)[1];
				// System.out.println("--versionSuffix:" + versionSuffix);
				int versionFull = Integer.parseInt(versionSuffix);
				// System.out.println("--VersionFull:" + versionFull);
				int version = versionFull / 2;
				// System.out.println("--version:" + version);
				Boolean isAdd = false; // true if is Deleted
				if (versionFull % 2 == 0) {
					isAdd = true;
				}

				/*
				 * OUTPUT RESULTS OF THE PAST VERSION
				 */
				if (numRows > 1 && (version != prevVersion)) {

					// iterate to prevent versions with no changes
					for (int n = 0; n < (version - prevVersion); n++) {
						ret.put(prevVersion + n, new ArrayList<String>(finalResults));
					}
				}
				prevVersion = version;

				// assume we have element1 and element2
				// variables as
				// a response
				String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);

				// System.out.println("****** RowResult: " + rowResult);
				if (isAdd) {
					finalResults.add(rowResult);
					// System.out.println("ADDED");
				} else {
					finalResults.remove(rowResult);
					// System.out.println("DEL");
				}
				// System.out.println("TotalRows (up to version " + version + "):" + numRows);

			}
			/*
			 * OUTPUT LAST RESULTS
			 */
			if (numRows >= 1) {

				// iterate to prevent versions with no changes
				int missingVersionsEnd = TOTALVERSIONS - prevVersion;
				if (!versionsStartWith0)
					missingVersionsEnd += 1;

				for (int n = 0; n < missingVersionsEnd; n++) {
					ret.put(prevVersion + n, new ArrayList<String>(finalResults));
				}

			}

		} finally {
			long endTime = System.currentTimeMillis();
			if (measureTime) {
				PrintWriter pw;
				try {
					pw = new PrintWriter(new File(outputTime));
					pw.println(endTime - startTime);
					pw.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}

			}
			qexec.close();
		}
		return ret;
	}

	/**
	 * Reads input file with a Resource, and gets all result of the lookup of the provided Resource with the provided rol (Subject, Predicate, Object)
	 * for all versions
	 * 
	 * @param queryFile
	 * @param rol
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	public ArrayList<Map<Integer, ArrayList<String>>> bulkAllVerQuerying(String queryFile, String rol) throws InterruptedException,
			ExecutionException, IOException {
		ArrayList<Map<Integer, ArrayList<String>>> ret = new ArrayList<Map<Integer, ArrayList<String>>>();

		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}
		DescriptiveStatistics total = new DescriptiveStatistics();

		Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;

		while ((line = br.readLine()) != null) {
			Map<Integer, ArrayList<String>> AllSolutions = new HashMap<Integer, ArrayList<String>>();

			/*
			 * warmup the system
			 */
			warmup();

			String[] parts = line.split(" ");

			// String element = parts[0];

			String queryString = QueryUtils.createLookupQueryGraph(rol, parts);
			Query query = QueryFactory.create(queryString);
			// System.out.println("query"+query);
			long startTime = System.currentTimeMillis();
			QueryExecution qexec = QueryExecutionFactory.create(query, dataset);

			ResultSet results = qexec.execSelect();

			HashSet<String> finalResults = new HashSet<String>();

			Iterator<QuerySolution> sortResults = orderedResultSet(results, "graph");
			int numRows = 0;
			int prevVersion = -1;
			while (sortResults.hasNext()) {
				numRows++;
				QuerySolution soln = sortResults.next();
				// assume we have a graph variable as a response
				String graphResponse = soln.getResource("graph").toString();
				// System.out.println("--graphResponse:" + graphResponse);
                if (!graphResponse.contains(prefixGraphsVersions)) break;
				String versionSuffix = graphResponse.split(prefixGraphsVersions)[1];
				// System.out.println("--versionSuffix:" + versionSuffix);
				int versionFull = Integer.parseInt(versionSuffix);
				// System.out.println("--VersionFull:" + versionFull);
				int version = versionFull / 2;
				// System.out.println("--version:" + version);
				Boolean isAdd = false; // true if is Deleted
				if (versionFull % 2 == 0) {
					isAdd = true;
				}

				/*
				 * OUTPUT RESULTS OF THE PAST VERSION
				 */
				if (numRows > 1 && (version != prevVersion)) {

					// iterate to prevent versions with no changes
					for (int n = 0; n < (version - prevVersion); n++) {
						AllSolutions.put(prevVersion + n, new ArrayList<String>(finalResults));
					}

				}
				prevVersion = version;

				// assume we have element1 and element2
				// variables as
				// a response
				if (!askQuery) {
					String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);

					// System.out.println("****** RowResult: " + rowResult);
					if (isAdd) {
						finalResults.add(rowResult);
						// System.out.println("ADDED");
					} else {
						finalResults.remove(rowResult);
						// System.out.println("DEL");
					}
				}
				else{
					if (isAdd) {
						finalResults.add("true");
						finalResults.remove("false");
						// System.out.println("ADDED");
					} else {
						finalResults.add("false");
						finalResults.remove("true");
						// System.out.println("DEL");
					}
				}
				// System.out.println("TotalRows (up to version " + version +
				// "):"
				// + numRows);

			}
			/*
			 * OUTPUT LAST RESULTS
			 */
			if (numRows >= 1) {

				// iterate to prevent versions with no changes
				int missingVersionsEnd = TOTALVERSIONS - prevVersion;
				if (!versionsStartWith0)
					missingVersionsEnd += 1;

				for (int n = 0; n < missingVersionsEnd; n++) {
					AllSolutions.put(prevVersion + n, new ArrayList<String>(finalResults));
				}

			}
			ret.add(AllSolutions);

			long endTime = System.currentTimeMillis();
			// System.out.println("Time:" + (endTime - startTime));
			// printStreamTime.println(queryFile + "," + (endTime - startTime));
			qexec.close();
			total.addValue((endTime - startTime));

			// vStats.get(versionQuery).addValue((endTime-startTime));
		}

		br.close();
		if (measureTime) {
			// PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-dynver-" + inputFile.getName()));
			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("##name, min, mean, max, stddev, count, total");
			pw.println("tot," + total.getMin() + "," + total.getMean() + "," + total.getMax() + "," + total.getStandardDeviation() + ","
					+ total.getN() + " " + total.getSum());
			pw.close();
		}
		return ret;
	}

	/**
	 * Warmup the system
	 * 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void warmup() throws InterruptedException, ExecutionException {
		Query query = QueryFactory.create(createWarmupQuery());
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		ResultSet results = qexec.execSelect();

		Iterator<QuerySolution> sortResults = orderedResultSet(results, "graph");
		HashSet<String> finalResults = new HashSet<String>();
		while (sortResults.hasNext()) {
			QuerySolution soln = sortResults.next();
			String rowResult = QueryUtils.serializeSolution(soln);

			// System.out.println("****** RowResult: " + rowResult);
			finalResults.add(rowResult);
		}
		qexec.close();

	}

	private static String createWarmupQuery() {
		String queryString = "SELECT ?element1 ?element2 ?element3 ?graph WHERE { " + "GRAPH ?graph{" + " ?element1 ?element2 ?element3 ."

		+ "}}" + "LIMIT 100";

		return queryString;
	}

	/**
	 * close Jena TDB and release resources
	 * 
	 * @throws RuntimeException
	 */
	public void close() throws RuntimeException {
		dataset.end();
	}
}
