package org.ai.wu.ac.at.tdbArchive.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.ai.wu.ac.at.tdbArchive.api.RDFArchive;
import org.ai.wu.ac.at.tdbArchive.solutions.DiffSolution;
import org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query;
import org.ai.wu.ac.at.tdbArchive.utils.QueryResult;
import org.ai.wu.ac.at.tdbArchive.utils.QueryUtils;
import org.ai.wu.ac.at.tdbArchive.utils.TaskCallable;
import org.ai.wu.ac.at.tdbArchive.utils.TaskThread;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jena.query.*;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.util.FileManager;

public class JenaTDBArchive_CB implements RDFArchive {

	private int TOTALVERSIONS = 0;

	private String outputTime = "timeApp.txt";
	private Boolean measureTime = false;
	private Map<Integer, Dataset> dataset_adds;
	private Map<Integer, Dataset> dataset_dels;

	/**
	 * @param outputTime
	 */
	public void setOutputTime(String outputTime) {
		this.outputTime = outputTime;
		this.measureTime = true;
	}

	public JenaTDBArchive_CB() {
		/*
		 * Load all datasets
		 */
		dataset_adds = new TreeMap<Integer, Dataset>();
		dataset_dels = new TreeMap<Integer, Dataset>();
		this.measureTime = false;
	}

	/**
	 * Load Jena TDB from directory
	 * 
	 * @param directory
	 * @throws RuntimeException
	 */
	public void load(String directory) {
		// Initialize Jena
		ARQ.init();
		FileManager fm = FileManager.get();
		fm.addLocatorClassLoader(JenaTDBArchive_query.class.getClassLoader());
		/*
		 * Load all datasets
		 */

		File folder = new File(directory);
		if (!folder.isDirectory())
			throw new RuntimeException("tdbfolder " + folder + " is not a directory");
		for (File fileEntry : folder.listFiles()) {
			int fileVersion = Integer.parseInt(fileEntry.getName());
			// System.out.println("... Loading TDB version" + fileVersion + " as ADD");
			dataset_adds.put(fileVersion, TDBFactory.createDataset(directory + "/" + fileVersion + "/add/"));
			// System.out.println("... Loading TDB version" + fileVersion + " as DEL");
			dataset_dels.put(fileVersion, TDBFactory.createDataset(directory + "/" + fileVersion + "/del/"));
			TOTALVERSIONS++;
		}
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

		ResultSet[] results_adds = new ResultSet[TOTALVERSIONS];
		ResultSet[] results_dels = new ResultSet[TOTALVERSIONS];

		/**
		 * START PARALELL
		 */

		Collection<Callable<QueryResult>> tasks = new ArrayList<Callable<QueryResult>>();
		// for the (initial version +1) up to the post version
		// Note that it is +1 in order to compute the difference with the following one

		for (int i = startVersionQuery + 1; i <= endVersionQuery; i++) {

			tasks.add(new TaskCallable(query, dataset_adds.get(i), i, true));
			tasks.add(new TaskCallable(query, dataset_dels.get(i), i, false));
		}
		ExecutorService executor = Executors.newFixedThreadPool(TOTALVERSIONS);
		List<Future<QueryResult>> results = executor.invokeAll(tasks);

		/**
		 * END PARALELL
		 */

		for (Future<QueryResult> result : results) {
			QueryResult res = result.get();
			// System.out.println("version:" + res.getVersion());
			// System.out.println("version:" + res.getSol().hasNext());
			if (res.getIsAdd())
				results_adds[res.getVersion()] = res.getSol();
			else
				results_dels[res.getVersion()] = res.getSol();
		}
		/**
		 * END PARALELL
		 */

		for (int i = (startVersionQuery + 1); i <= endVersionQuery; i++) {

			while (results_adds[i].hasNext()) {
				QuerySolution soln = results_adds[i].next();
				String rowResult = QueryUtils.serializeSolution(soln);

				// System.out.println("****** RowResult ADD: " + rowResult);

				if (!finalDels.remove(rowResult))
					finalAdds.add(rowResult);

			}
			while (results_dels[i].hasNext()) {
				QuerySolution soln = results_dels[i].next();
				String rowResult = QueryUtils.serializeSolution(soln);

				// System.out.println("****** RowResult DEL: " + rowResult);

				if (!finalAdds.remove(rowResult))
					finalDels.add(rowResult);

			}
		}

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
		executor.shutdown(); // always reclaim resources
		// System.out.println("Time:" + (endTime - startTime));
		return new DiffSolution(finalAdds, finalDels);

	}

	/**
	 * @param queryFile
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	// TODO Review, Not sure if this is implemented correctly in CB
	private void bulkAllChangeQuerying(String queryFile, String rol) throws InterruptedException, ExecutionException, IOException {

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

		DescriptiveStatistics total = new DescriptiveStatistics();

		Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;

		while ((line = br.readLine()) != null) {
			Map<Integer, DiffSolution> solutions = new HashMap<Integer, DiffSolution>();

			String[] parts = line.split(" ");
			// String element = parts[0]; //we take all parts in order to process all TP patterns

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
				// System.out.println("versionQuery:" + versionQuery + " ; postQuery:" + postversionQuery);

				String queryString = QueryUtils.createLookupQuery(rol, parts);

				Query query = QueryFactory.create(queryString);

				long startTime = System.currentTimeMillis();

				/**
				 * START PARALELL
				 */

				TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
				TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

				Set<TaskThread> a = new HashSet<TaskThread>();
				for (int i = versionQuery + 1; i <= postversionQuery; i++) {
					TaskThread task_add = new TaskThread(query, dataset_adds.get(i), i, true, results_adds, askQuery);
					TaskThread task_del = new TaskThread(query, dataset_dels.get(i), i, false, results_dels, askQuery);
					task_add.start();
					a.add(task_add);
					task_del.start();
					a.add(task_del);
				}
				for (TaskThread an : a) {
					try {
						an.join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				/**
				 * END PARALELL
				 */

				for (int i = (versionQuery + 1); i <= postversionQuery; i++) {
					if (!askQuery) {
						while (results_adds.get(i).getSol().hasNext()) {
							QuerySolution soln = results_adds.get(i).getSol().next();
							String rowResult = QueryUtils.serializeSolution(soln);

							// System.out.println("****** RowResult ADD: " +
							// rowResult);

							if (!finalDels.remove(rowResult))
								finalAdds.add(rowResult);

						}
						while (results_dels.get(i).getSol().hasNext()) {
							QuerySolution soln = results_dels.get(i).getSol().next();
							String rowResult = QueryUtils.serializeSolution(soln);

							// System.out.println("****** RowResult DEL: " +
							// rowResult);

							if (!finalAdds.remove(rowResult))
								finalDels.add(rowResult);

						}
					} else {
						Boolean sol_add = results_adds.get(i).getSolAsk();
						if (!finalDels.remove(sol_add.toString()))
							finalAdds.add(sol_add.toString());
						Boolean sol_del = results_dels.get(i).getSolAsk();
						if (!finalAdds.remove(sol_del.toString()))
							finalDels.add(sol_del.toString());
					}
				}

				solutions.put(postversionQuery, new DiffSolution(finalAdds, finalDels));
				long endTime = System.currentTimeMillis();
				// System.out.println("Time:" + (endTime - startTime));
				total.addValue((endTime - startTime));
				vStats.get(index).addValue((endTime - startTime));

				// printStreamTime.println(queryFile + "," + (endTime -
				// startTime));

			}
			ret.add(solutions);
		}
		if (measureTime) {
			// String outFile = outputDIR + "/res-dyndiff-" + inputFile.getName();
			// if (jump > 0)
			// outFile = outputDIR + "/res-dyndiff-jump" + jump + "-" + inputFile.getName();
			// PrintWriter pw = new PrintWriter(new File(outFile));
			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("##bucket, min, mean, max, stddev, count, total");
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

		ArrayList<String> ret = materializeQuery(version, query);

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
			String queryString = QueryUtils.createLookupQuery(rol, element);

			// System.out.println("Query at version " + staticVersionQuery);
			// System.out.println(queryString);

			long startTime = System.currentTimeMillis();
			Query query = QueryFactory.create(queryString);

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
			// String element = parts[0]; //we take all parts in order to process all TP patterns

			/*
			 * warmup the system
			 */
			warmup();

			String queryString = QueryUtils.createLookupQuery(rol, parts);
			Map<Integer, ArrayList<String>> solutions = new HashMap<Integer, ArrayList<String>>();
			for (int i = 0; i < TOTALVERSIONS; i++) {
				// System.out.println("Query at version " + i);
				// System.out.println(queryString);

				Query query = QueryFactory.create(queryString);
				// FIXME TEST cold scenario
				// String[] commands = {"/bin/sh", "-c" , "sync && echo 3 > /proc/sys/vm/drop_caches"};
				// Process pr = Runtime.getRuntime().exec(commands);
				// pr.waitFor();

				long startTime = System.currentTimeMillis();
				if (!askQuery)
					solutions.put(i, materializeQuery(i, query));
				else
					solutions.put(i, materializeASKQuery(i, query));
				long endTime = System.currentTimeMillis();
				// System.out.println("Time:" + (endTime - startTime));
				vStats.get(i).addValue((endTime - startTime));
			}
			ret.add(solutions);

		}
		br.close();

		if (measureTime) {
			// PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-dynmat-" + inputFile.getName()));
			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("##ver, min, mean, max, stddev, count, total");
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
	private ArrayList<String> materializeQuery(int staticVersionQuery, Query query) throws InterruptedException, ExecutionException {

		// ArrayList<String> ret = new ArrayList<String>();

		/**
		 * START PARALELL
		 */

		TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
		TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

		Set<TaskThread> a = new HashSet<TaskThread>();
		for (int i = 0; i <= staticVersionQuery; i++) {
			TaskThread task_add = new TaskThread(query, dataset_adds.get(i), i, true, results_adds);
			TaskThread task_del = new TaskThread(query, dataset_dels.get(i), i, false, results_dels);
			task_add.start();
			a.add(task_add);
			task_del.start();
			a.add(task_del);
		}
		for (TaskThread an : a) {
			try {
				an.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * END PARALELL
		 */

		HashSet<String> finalResults = new HashSet<String>();

		for (int i = 0; i <= staticVersionQuery; i++) {
			// System.out.println("Iterating results " + i);
			while (results_adds.get(i).getSol().hasNext()) {
				QuerySolution soln = results_adds.get(i).getSol().next();
				String rowResult = QueryUtils.serializeSolution(soln);
				finalResults.add(rowResult);
				// System.out.println("****** RowResult: " + rowResult);
			}
			while (results_dels.get(i).getSol().hasNext()) {
				QuerySolution soln = results_dels.get(i).getSol().next();
				String rowResult = QueryUtils.serializeSolution(soln);

				// System.out.println("****** RowResult: " + rowResult);
				finalResults.remove(rowResult);
			}
		}

		return new ArrayList<String>(finalResults);

	}

	/**
	 * @param staticVersionQuery
	 * @param query
	 * @return
	 */
	private ArrayList<String> materializeASKQuery(int staticVersionQuery, Query query) throws InterruptedException, ExecutionException {

		// ArrayList<String> ret = new ArrayList<String>();

		/**
		 * START PARALELL
		 */

		TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
		TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

		Set<TaskThread> a = new HashSet<TaskThread>();
		for (int i = 0; i <= staticVersionQuery; i++) {
			TaskThread task_add = new TaskThread(query, dataset_adds.get(i), i, true, results_adds, true);
			TaskThread task_del = new TaskThread(query, dataset_dels.get(i), i, false, results_dels, true);
			task_add.start();
			a.add(task_add);
			task_del.start();
			a.add(task_del);
		}
		for (TaskThread an : a) {
			try {
				an.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * END PARALELL
		 */

		HashSet<String> finalResults = new HashSet<String>();

		for (int i = 0; i <= staticVersionQuery; i++) {
			finalResults.add(results_adds.get(i).getSolAsk().toString());
			finalResults.remove(results_dels.get(i).getSolAsk().toString());

		}

		return new ArrayList<String>(finalResults);

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
		ResultSet[] results_adds = new ResultSet[TOTALVERSIONS];
		ResultSet[] results_dels = new ResultSet[TOTALVERSIONS];

		/**
		 * START PARALELL
		 */

		Collection<Callable<QueryResult>> tasks = new ArrayList<Callable<QueryResult>>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			if (dataset_adds.get(i) != null) {
				tasks.add(new TaskCallable(query, dataset_adds.get(i), i, true));
			}
			if (dataset_dels.get(i) != null) {
				tasks.add(new TaskCallable(query, dataset_dels.get(i), i, false));
			}
		}
		ExecutorService executor = Executors.newFixedThreadPool(TOTALVERSIONS);
		List<Future<QueryResult>> results = executor.invokeAll(tasks);

		/**
		 * END PARALELL
		 */
		HashSet<String> finalResults = new HashSet<String>();
		for (Future<QueryResult> result : results) {
			QueryResult res = result.get();
			// System.out.println("version:" + res.version);
			// System.out.println("version:" + res.sol.hasNext());
			if (res.getIsAdd())
				results_adds[res.getVersion()] = res.getSol();
			else
				results_dels[res.getVersion()] = res.getSol();
		}
		// for all versions
		for (int i = 0; i < TOTALVERSIONS; i++) {

			// System.out.println("Computing results ADD for version " + i);
			while (results_adds[i].hasNext()) {
				QuerySolution soln = results_adds[i].next();
				String rowResult = QueryUtils.serializeSolution(soln);

				finalResults.add(rowResult);
				// System.out.println("ADD:" + rowResult);
			}
			// System.out.println("computing results DEL for version " + i);
			while (results_dels[i].hasNext()) {
				QuerySolution soln = results_dels[i].next();
				String rowResult = QueryUtils.serializeSolution(soln);
				// System.out.println("DEL:" + rowResult);
				finalResults.remove(rowResult);
			}

			/*
			 * OUTPUT RESULTS OF THE CURRENT VERSION
			 */

			/*
			 * Iterator<String> it = finalResults.iterator(); String res; while (it.hasNext()) { res = it.next(); // element is the response
			 * //printStream.println(i + "," + res); // System.out.println("count:" + count); }
			 */
			// System.out.println("Insert in i=" + i + " :" + finalResults);
			ret.put(i, new ArrayList<String>(finalResults));
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

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}
		DescriptiveStatistics total = new DescriptiveStatistics();

		Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;

		while ((line = br.readLine()) != null) {
			Map<Integer, ArrayList<String>> AllSolutions = new HashMap<Integer, ArrayList<String>>();

			String[] parts = line.split(" ");
			// String element = parts[0]; //we take all parts in order to process all TP patterns

			/*
			 * warmup the system
			 */
			warmup();

			String queryString = QueryUtils.createLookupQuery(rol, parts);
			Query query = QueryFactory.create(queryString);

			long startTime = System.currentTimeMillis();

			/**
			 * START PARALELL
			 */

			TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
			TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

			Set<TaskThread> a = new HashSet<TaskThread>();
			for (int i = 0; i < TOTALVERSIONS; i++) {
				TaskThread task_add = new TaskThread(query, dataset_adds.get(i), i, true, results_adds, askQuery);
				TaskThread task_del = new TaskThread(query, dataset_dels.get(i), i, false, results_dels, askQuery);
				task_add.start();
				a.add(task_add);
				task_del.start();
				a.add(task_del);
			}
			for (TaskThread an : a) {
				try {
					an.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			/**
			 * END PARALELL
			 */

			HashSet<String> finalResults = new HashSet<String>();

			// for all versions
			for (int i = 0; i < TOTALVERSIONS; i++) {

				if (!askQuery) {
					// System.out.println("computing results ADD for version " + i);
					while (results_adds.get(i).getSol().hasNext()) {
						QuerySolution soln = results_adds.get(i).getSol().next();
						String rowResult = QueryUtils.serializeSolution(soln);

						finalResults.add(rowResult);
					}
					// System.out.println("computing results ADD for version " + i);
					while (results_dels.get(i).getSol().hasNext()) {
						QuerySolution soln = results_dels.get(i).getSol().next();
						String rowResult = QueryUtils.serializeSolution(soln);

						finalResults.remove(rowResult);
					}
				}
				else{
					finalResults.add(results_adds.get(i).getSolAsk().toString());
					finalResults.remove(results_dels.get(i).getSolAsk().toString());
				}

				/*
				 * OUTPUT RESULTS OF THE CURRENT VERSION
				 */

				AllSolutions.put(i, new ArrayList<String>(finalResults));
			}
			long endTime = System.currentTimeMillis();
			// System.out.println("Time:" + (endTime - startTime));
			total.addValue((endTime - startTime));

			// printStreamTime.println(queryFile + "," + (endTime - startTime));

			ret.add(AllSolutions);
		}

		br.close();
		if (measureTime) {
			// PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-dynver-" + inputFile.getName()));
			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("##name, min, mean, max, stddev, count,total");
			pw.println("tot," + total.getMin() + "," + total.getMean() + "," + total.getMax() + "," + total.getStandardDeviation() + ","
					+ total.getN()+" "+total.getSum());
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

		/**
		 * START PARALELL
		 */

		TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
		TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

		Set<TaskThread> a = new HashSet<TaskThread>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			TaskThread task_add = new TaskThread(query, dataset_adds.get(i), i, true, results_adds);
			TaskThread task_del = new TaskThread(query, dataset_dels.get(i), i, false, results_dels);
			task_add.start();
			a.add(task_add);
			task_del.start();
			a.add(task_del);
		}
		for (TaskThread an : a) {
			try {
				an.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * END PARALELL
		 */
		HashSet<String> finalResults = new HashSet<String>();

		for (int i = 0; i < TOTALVERSIONS; i++) {
			// System.out.println("Iterating results " + i);
			while (results_adds.get(i).getSol().hasNext()) {
				QuerySolution soln = results_adds.get(i).getSol().next();
				// System.out.println("++ ADDED in Version:" + i);
				String rowResult = QueryUtils.serializeSolution(soln);

				// System.out.println("****** RowResult: " + rowResult);
				finalResults.add(rowResult);
			}
			while (results_dels.get(i).getSol().hasNext()) {
				QuerySolution soln = results_dels.get(i).getSol().next();
				// System.out.println("-- DEL in Version:" + i);
				String rowResult = QueryUtils.serializeSolution(soln);

				// System.out.println("****** RowResult: " + rowResult);
				finalResults.remove(rowResult);
			}
		}

	}

	private static String createWarmupQuery() {
		String queryString = "SELECT ?element1 ?element2 ?element3 WHERE { " + " ?element1 ?element2 ?element3 ."

		+ "}" + "LIMIT 100";

		return queryString;
	}

	/**
	 * close Jena TDB and release resources
	 * 
	 * @throws RuntimeException
	 */
	public void close() throws RuntimeException {
		for (int i = 0; i < TOTALVERSIONS; i++) {
			dataset_adds.get(i).end();
			dataset_dels.get(i).end();
		}

	}
}
