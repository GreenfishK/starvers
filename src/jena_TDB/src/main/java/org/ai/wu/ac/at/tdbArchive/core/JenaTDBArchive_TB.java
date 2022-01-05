package org.ai.wu.ac.at.tdbArchive.core;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.ai.wu.ac.at.tdbArchive.solutions.DiffSolution;
import org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query;
import org.ai.wu.ac.at.tdbArchive.utils.QueryResult;
import org.ai.wu.ac.at.tdbArchive.utils.QueryUtils;
import org.ai.wu.ac.at.tdbArchive.utils.TaskCallable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDBLoader;
import org.apache.jena.tdb.setup.DatasetBuilderStd;
import org.apache.jena.tdb.store.DatasetGraphTDB;
import org.apache.jena.util.FileManager;
import org.apache.jena.tdb.base.file.Location;
import org.ai.wu.ac.at.tdbArchive.api.JenaTDBArchive;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.util.thread.Scheduler;

//import org.apache.jena.system.JenaSystem;

public class JenaTDBArchive_TB implements JenaTDBArchive {
	private static final Logger logger = LogManager.getLogger(JenaTDBArchive_TB.class);

	private int TOTALVERSIONS = 0;
	private String outputTime = "timeApp.txt";
	private FusekiServer server;
	private RDFConnection conn;
	private Boolean measureTime = false;
	private static String metadataVersions = "<http://www.w3.org/2002/07/owl#versionInfo>";

	// private static String metadataVersions = "<http://example.org/isVersion>";

	/**
	 * @param outputTime
	 */
	public void setOutputTime(String outputTime) {
		this.outputTime = outputTime;
		this.measureTime = true;
	}

	public JenaTDBArchive_TB() throws FileNotFoundException {
		this.measureTime = false;
	}

	/**
	 * Load Jena TDB from directory
	 * 
	 * @param directory The directory of multiple rdf files
	 * or location of a single rdf file (e.g. ttl or nq).
	 */
	public void load(String directory) {
		// Initialize Jena
		ARQ.init();
		FileManager fm = FileManager.get();
		fm.addLocatorClassLoader(JenaTDBArchive_query.class.getClassLoader());

		// Create a TDB persistent dataset in tmp/TDB/currentTimestamp and load the .nq file into it.
		String tdb_loc = "target/TDB"; //+ currentTimestamp;
		DatasetGraphTDB dsg = DatasetBuilderStd.create(Location.create(tdb_loc));
		logger.info(String.format("If you are using docker the TDB dataset will be located " +
				"in /var/lib/docker/overlay2/<buildID>/diff/%s", tdb_loc));
		InputStream in = fm.open(directory);
		TDBLoader.load(dsg, in, Lang.NQ,false, true);

		Dataset dataset;
		try {
			// Create a dataset object from the persistent TDB dataset
			dataset = TDBFactory.createDataset(tdb_loc); //dsg.getDefaultGraphTDB().getDataset();

			// Write dataset info file if the location of file where the query performances will be stored is given
			// Writes in the same directory as the query performance file
			if(!this.outputTime.equals("")) {
				File datasetLogFileDir = new File(this.outputTime).getParentFile();
				long tbdDirSize = FileUtils.sizeOfDirectory(new File(tdb_loc));

				logger.debug(datasetLogFileDir);
				String datasetLogFile = datasetLogFileDir + "/dataset_infos_tb.csv";
				logger.debug(datasetLogFile);
				File f = new File(datasetLogFile);
				PrintWriter pw;
				if ( f.exists() && !f.isDirectory() ) {
					pw = new PrintWriter(new FileOutputStream(datasetLogFile, true));
				}
				else {
					pw = new PrintWriter(datasetLogFile);
				}
				pw.append("ds_name, tdb_ds_size\n");
				pw.append("bearb_jena_tdb_tb" + "," + tbdDirSize + "\n");
				pw.close();
				logger.info(String.format("Writing dataset logs to directory: %s", datasetLogFile));
			}

			// Create a fuseki server, load the dataset into the repository
			// http://localhost:3030/in_memory_server/sparql and connect to it.
			server = FusekiServer.create()
					.add("/in_memory_server", dataset)
					.build();
			server.start();
			conn = RDFConnection.connect(String.format("http://localhost:%d/in_memory_server/sparql",
					server.getHttpPort()));
			dataset.end();
		} catch (Exception e) {
			e.printStackTrace();
		}

		//Get the number of versions in the dataset (number of named graphs)
		String query_string = QueryUtils.getNumGraphVersions(metadataVersions);
		//Query query = QueryFactory.create(queryGraphs);
		QueryExecution qExec = conn.query(query_string);
		ResultSet results = qExec.execSelect();
		while (results.hasNext()) {
			QuerySolution soln = results.next();
			String numVersions = soln.getLiteral("numVersions").getLexicalForm();
			TOTALVERSIONS = Integer.parseInt(numVersions);
			logger.info("Totalversions: " + TOTALVERSIONS);
		}
		qExec.close();
		conn.close();
	}

	/**
	 * Gets the diff of the provided query between the two given versions
	 * 
	 * @param startVersionQuery
	 * @param endVersionQuery
	 * @param TP
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public DiffSolution diffQuerying(int startVersionQuery, int endVersionQuery, String TP) throws InterruptedException, ExecutionException {

		String fullQueryStart = QueryUtils.createLookupQueryAnnotatedGraph(TP, startVersionQuery, metadataVersions);
		Query queryStart = QueryFactory.create(fullQueryStart);
		String fullQueryEnd = QueryUtils.createLookupQueryAnnotatedGraph(TP, endVersionQuery, metadataVersions);
		Query queryEnd = QueryFactory.create(fullQueryEnd);
		HashSet<String> finalAdds = new HashSet<String>();
		HashSet<String> finalDels = new HashSet<String>();

		long startTime = System.currentTimeMillis();

		/**
		 * START PARALELL
		 */

		Collection<Callable<QueryResult>> tasks = new ArrayList<Callable<QueryResult>>();
		// // for the (initial version +1) up to the post version
		// // Note that it is +1 in order to compute the difference with the
		// // following one

		tasks.add(new TaskCallable(queryStart, conn.fetchDataset(), startVersionQuery, true));
		tasks.add(new TaskCallable(queryEnd, conn.fetchDataset(), endVersionQuery, true));
		ExecutorService executor = Executors.newFixedThreadPool(TOTALVERSIONS);
		List<Future<QueryResult>> results = executor.invokeAll(tasks);

		/**
		 * END PARALELL
		 */

		HashSet<String> finalResultsStart = new HashSet<String>();
		HashSet<String> finalResultsEnd = new HashSet<String>();
		for (Future<QueryResult> result : results) {
			QueryResult res = result.get();
			// system.out.println("version:" + res.version);
			if (res.getVersion() == startVersionQuery) {
				while (res.getSol().hasNext()) {
					QuerySolution soln = res.getSol().next();
					String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);

					// system.out.println("****** RowResult finalResultsStart: " + rowResult);
					finalResultsStart.add(rowResult);
				}
			} else {
				while (res.getSol().hasNext()) {
					QuerySolution soln = res.getSol().next();
					String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);

					// system.out.println("****** RowResult finalResultsEnd: " + rowResult);
					finalResultsEnd.add(rowResult);
				}
			}
		}

		Iterator<String> it = finalResultsStart.iterator();
		String res;
		while (it.hasNext()) {
			res = it.next();
			if (!finalResultsEnd.contains(res)) {
				// System.out.println("final del:" + res);
				finalDels.add(res);
			}
			// element is the response
		}
		it = finalResultsEnd.iterator();
		while (it.hasNext()) {
			res = it.next();

			if (!finalResultsStart.contains(res)) {
				// System.out.println("final add:" + res);
				finalAdds.add(res);
			}
			// element is the response
		}

		long endTime = System.currentTimeMillis();
		executor.shutdown(); // always reclaim resources
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

		Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}

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
				ArrayList<String> finalAdds = new ArrayList<String>();
				ArrayList<String> finalDels = new ArrayList<String>();
				int versionQuery = index;
				int postversionQuery = versionQuery + 1;
				if (jump > 0) {
					postversionQuery = Math.min((index + 1) * jump, TOTALVERSIONS - 1);
					versionQuery = 0;
				}
				logger.info("versionQuery:" + versionQuery + " ; postQuery:" + postversionQuery);

				String queryStringStart = QueryUtils.createLookupQueryAnnotatedGraph(rol, parts, versionQuery, metadataVersions);
				String queryStringEnd = QueryUtils.createLookupQueryAnnotatedGraph(rol, parts, postversionQuery, metadataVersions);
                int limit = QueryUtils.getLimit(parts);
				long startTime = System.currentTimeMillis();
				QueryExecution qexecStart = conn.query(queryStringStart);
				QueryExecution qexecEnd = conn.query(queryStringEnd);
				HashSet<String> finalResultsStart = new HashSet<String>();
				HashSet<String> finalResultsEnd = new HashSet<String>();
				if (!askQuery) {
					ResultSet resultsStart = qexecStart.execSelect();

					QuerySolution soln = null;
					while (resultsStart.hasNext() && limit-- > 0) {
						soln = resultsStart.next();
						String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);
						// System.out.println("solutionStart: "+rowResult);
						finalResultsStart.add(rowResult);
					}

					ResultSet resultsEnd = qexecEnd.execSelect();

					while (resultsEnd.hasNext() && limit-- > 0) {
						soln = resultsEnd.next();
						String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);
						// System.out.println("solutionEnd: "+rowResult);
						finalResultsEnd.add(rowResult);
						if (!finalResultsStart.contains(rowResult)) {
							// result has been added
							// System.out.println("add: " + rowResult);
							finalAdds.add(rowResult);

						}

					}
					// check potential results deleted

					for (String solStart : finalResultsStart) {
						if (!finalResultsEnd.contains(solStart)) {
							// result has been deleted
							// System.out.println("del: " + solStart);
							finalDels.add(solStart);

						}
					}

				} else {
					Boolean resultStart = qexecStart.execAsk();
					finalResultsStart.add(resultStart.toString());
					Boolean resultsEnd = qexecEnd.execAsk();
					finalResultsEnd.add(resultsEnd.toString());
					if (!finalResultsStart.contains(resultsEnd.toString())) {
						finalAdds.add(resultsEnd.toString());
					}
					if (!finalResultsEnd.contains(resultStart.toString())) {
						finalDels.add(resultStart.toString());
					}

				}
				qexecStart.close();
				qexecEnd.close();

				solutions.put(postversionQuery, new DiffSolution(finalAdds, finalDels));
				long endTime = System.currentTimeMillis();
				// System.out.println("Time:" + (endTime - startTime));
				total.addValue((endTime - startTime));
				vStats.get(index).addValue((endTime - startTime));

			}
			ret.add(solutions);
		}
		if (measureTime) {
			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("##bucket, min, mean, max, stddev, count, sum");
			for (Entry<Integer, DescriptiveStatistics> ent : vStats.entrySet()) {
				pw.println(ent.getKey() + " " + ent.getValue().getMin() + " " + ent.getValue().getMean() + " " + ent.getValue().getMax() + " "
						+ ent.getValue().getStandardDeviation() + " " + ent.getValue().getN()+" "+ent.getValue().getSum());
			}
			pw.println("tot," + total.getMin() + "," + total.getMean() + "," + total.getMax() + "," + total.getStandardDeviation() + ","
					+ total.getN());
			pw.close();
		}
		br.close();
		return ret;
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
		long startTime = System.currentTimeMillis();

		ArrayList<String> ret = materializeQuery(version, query, Integer.MAX_VALUE);

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

			// System.out.println("Query at version " + staticVersionQuery);
			String queryString = QueryUtils.createLookupQueryAnnotatedGraph(rol, element, staticVersionQuery, metadataVersions);

			Query query = QueryFactory.create(queryString);
			long startTime = System.currentTimeMillis();

			ret.add(materializeQuery(staticVersionQuery, query, Integer.MAX_VALUE));

			long endTime = System.currentTimeMillis();
			//System.out.println("bulkMatQuerying: Time:" + (endTime - startTime)); //DEBUG

			vStats.get(staticVersionQuery).addValue((endTime - startTime));

		}
		br.close();

		if (measureTime) {
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
		ArrayList<Map<Integer, ArrayList<String>>> ret = new ArrayList<>();
		warmup();

		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}

		boolean askQuery = rol.equalsIgnoreCase("SPO") && false;
		for (int lines = 0; (line = br.readLine()) != null; lines++) {
			String[] parts = line.split(" ");

			Map<Integer, ArrayList<String>> solutions = new HashMap<>();
			System.out.printf("Query %x%n", lines+1);
			for (int i = 0; i < TOTALVERSIONS; i++) {
				//System.out.println("Query at version: " + i); //DEBUG
				String queryString = QueryUtils.createLookupQueryAnnotatedGraph(rol, parts, i, metadataVersions);
                int limit = QueryUtils.getLimit(parts);
				//System.out.println(queryString); //DEBUG
				Query query = QueryFactory.create(queryString);

				long startTime = System.currentTimeMillis();
				if (true || !askQuery)
					solutions.put(i, materializeQuery(i, query, limit));
				else
					solutions.put(i, materializeASKQuery(i, query));
				long endTime = System.currentTimeMillis();
				vStats.get(i).addValue((endTime - startTime));

			}
			ret.add(solutions);
		}
		br.close();

		if (measureTime) {
			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("##ver, min, mean, max, stddev, count, sum");
			for (Entry<Integer, DescriptiveStatistics> ent : vStats.entrySet()) {
				pw.println(ent.getKey() + " " + ent.getValue().getMin() + " " + ent.getValue().getMean() + " " + ent.getValue().getMax() + " "
						+ ent.getValue().getStandardDeviation() + " " + ent.getValue().getN()+" "+ent.getValue().getSum());
			}
			pw.close();
		}
		return ret;
	}

	/**
	 * @param staticVersionQuery
	 * @param query
	 * @return
	 */
	private ArrayList<String> materializeQuery(int staticVersionQuery, Query query, int limit) throws InterruptedException, ExecutionException {
		conn = RDFConnection.connect(String.format("http://localhost:%d/in_memory_server/sparql", server.getHttpPort()));
		logger.info(String.format("Executing version %d", staticVersionQuery));
		QueryExecution qexec = conn.query(query.toString());
		ArrayList<String> ret = new ArrayList<>();
		ResultSet results = qexec.execSelect();
		Boolean higherVersion = false;

		while (results.hasNext() && !higherVersion && limit-- > 0) {
			// numRows++;
			QuerySolution soln = results.next();
			String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);
			// System.out.println(rowResult);
			ret.add(rowResult);
		}
		qexec.close();
		conn.close();

		return ret;
	}

	/**
	 * @param staticVersionQuery
	 * @param query
	 * @return
	 */
	private ArrayList<String> materializeASKQuery(int staticVersionQuery, Query query) throws InterruptedException, ExecutionException {
		QueryExecution qexec = conn.query(query);
		ArrayList<String> ret = new ArrayList<String>();
		qexec.getContext().set(ARQ.symLogExec, Explain.InfoLevel.NONE);

		Boolean result = qexec.execAsk();

		ret.add(result.toString());

		qexec.close();
		return ret;
	}

	static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
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
	 * @param TP
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public Map<Integer, ArrayList<String>> verQuery(String TP) throws InterruptedException, ExecutionException {
		Map<Integer, ArrayList<String>> ret = new HashMap<Integer, ArrayList<String>>();

		long startTime = System.currentTimeMillis();

		/**
		 * START PARALELL
		 */

		Collection<Callable<QueryResult>> tasks = new ArrayList<Callable<QueryResult>>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			String fullQuery = QueryUtils.createLookupQueryAnnotatedGraph(TP, i, metadataVersions);

			Query query = QueryFactory.create(fullQuery);
			tasks.add(new TaskCallable(query, conn.fetchDataset(), i, true));
		}
		ExecutorService executor = Executors.newFixedThreadPool(TOTALVERSIONS);
		List<Future<QueryResult>> results = executor.invokeAll(tasks);

		/**
		 * END PARALELL
		 */

		for (Future<QueryResult> result : results) {
			QueryResult res = result.get();
			ArrayList<String> solutions = new ArrayList<String>();
			while (res.getSol().hasNext()) {
				QuerySolution soln = res.getSol().next();
				String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);
				solutions.add(rowResult);
				// rowResult is the final result for version res.version
			}
			ret.put(res.getVersion(), solutions);
		}

		executor.shutdown(); // always reclaim resources
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

		Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}
		DescriptiveStatistics total = new DescriptiveStatistics();

		while ((line = br.readLine()) != null) {
			Map<Integer, ArrayList<String>> AllSolutions = new HashMap<Integer, ArrayList<String>>();

			/*
			 * warmup the system
			 */
			warmup();

			String[] parts = line.split(" ");

			// String element = parts[0];

			String queryString = QueryUtils.createLookupQueryAnnotatedGraph(rol, parts, metadataVersions);
            int limit = QueryUtils.getLimit(parts);

			System.out.println("the queryString: " + queryString);
			Query query = QueryFactory.create(queryString);
			long startTime = System.currentTimeMillis();
			QueryExecution qexec = conn.query(query);
			qexec.getContext().set(ARQ.symLogExec, Explain.InfoLevel.NONE);

			ResultSet results = qexec.execSelect();
			while (results.hasNext() && limit-- > 0) {
				System.out.println("SOLUTION");
				QuerySolution soln = results.next();
				// assume we have a graph variable as a response
				Literal version = (Literal) soln.get("version");
				int ver = version.getInt();
				String rowResult ="";
				if (!askQuery){
					rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);
				}
				else 
					 rowResult = "true";
				if (AllSolutions.get(ver) != null) {
					AllSolutions.get(ver).add(rowResult);
				} else {
					ArrayList<String> newSol = new ArrayList<String>();
					newSol.add(rowResult);
					AllSolutions.put(ver, newSol);
				}
				System.out.println("****** RowResult: " + rowResult);
				// System.out.println("version " + ver);
				// + numRows);

			}

			ret.add(AllSolutions);

			long endTime = System.currentTimeMillis();
			// System.out.println("Time:" + (endTime - startTime));
			// printStreamTime.println(queryFile + "," + (endTime - startTime));
			qexec.close();
			conn.close();
			total.addValue((endTime - startTime));

			// vStats.get(versionQuery).addValue((endTime-startTime));
		}

		br.close();
		if (measureTime) {
			// PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-dynver-" + inputFile.getName()));
			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("##name, min, mean, max, stddev, count, sum");
			pw.println("tot," + total.getMin() + "," + total.getMean() + "," + total.getMax() + "," + total.getStandardDeviation() + ","
					+ total.getN()+", "+total.getSum());
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
		logger.info("Running warmup query");
		conn = RDFConnection.connect(String.format("http://localhost:%d/in_memory_server/sparql", server.getHttpPort()));

		long startTime = System.currentTimeMillis();
		QueryExecution qexec = conn.query(createWarmupQuery());
		ResultSet results = qexec.execSelect();
		long endTime = System.currentTimeMillis();

		HashSet<String> finalResults = new HashSet<String>();
		Iterator<QuerySolution> sortResults = orderedResultSet(results, "graph");
		while (sortResults.hasNext()) {
			QuerySolution soln = sortResults.next();
			// assume we have a graph variable as a response
			String graphResponse = soln.getResource("graph").toString();
			finalResults.add(graphResponse);
		}
		System.out.println("Warmup Time:" + (endTime - startTime));
		System.out.println(finalResults);

		qexec.close();
		conn.close();
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
		server.stop();
	}
}
