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
import java.util.Iterator;
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

import org.ai.wu.ac.at.tdbArchive.api.JenaTDBArchive;
import org.ai.wu.ac.at.tdbArchive.solutions.DiffSolution;
import org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query;
import org.ai.wu.ac.at.tdbArchive.utils.QueryResult;
import org.ai.wu.ac.at.tdbArchive.utils.QueryUtils;
import org.ai.wu.ac.at.tdbArchive.utils.TaskCallable;
import org.ai.wu.ac.at.tdbArchive.utils.TaskThread;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.util.FileManager;

public class JenaTDBArchive_IC implements JenaTDBArchive {

	private int TOTALVERSIONS = 0;
	private String outputTime = "timeApp.txt";
	private Map<Integer, Dataset> datasets;
	private Boolean measureTime = false;

	/**
	 * @param outputTime
	 */
	public void setOutputTime(String outputTime) {
		this.outputTime = outputTime;
		this.measureTime = true;
	}

	public JenaTDBArchive_IC() {
		/*
		 * Load all datasets
		 */
		datasets = new TreeMap<Integer, Dataset>();
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
		FileManager fm = FileManager.get();
		fm.addLocatorClassLoader(JenaTDBArchive_query.class.getClassLoader());
		File folder = new File(directory);
		if (!folder.isDirectory())
			throw new RuntimeException("tdbfolder " + folder + " is not a directory");
		for (File fileEntry : folder.listFiles()) {
			int fileVersion = Integer.parseInt(fileEntry.getName());
			// System.out.println("... Loading TDB version " + fileVersion);
			datasets.put(fileVersion, TDBFactory.createDataset(directory + "/" + fileVersion));
			TOTALVERSIONS++;
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

			// system.out.println("Query at version " + staticVersionQuery);
			// system.out.println(queryString);

			long startTime = System.currentTimeMillis();
			Query query = QueryFactory.create(queryString);

			ret.add(materializeQuery(staticVersionQuery, query));

			long endTime = System.currentTimeMillis();
			// system.out.println("Time:" + (endTime - startTime));

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

		while ((line = br.readLine()) != null) {
			String[] parts = line.split(" ");
			// String element = parts[0]; //we take all parts in order to process all TP patterns

			/*
			 * warmup the system
			 */
			warmup();

			String queryString = QueryUtils.createLookupQuery(rol, parts);
			//System.out.println("queryString:" + queryString);
			Map<Integer, ArrayList<String>> solutions = new HashMap<Integer, ArrayList<String>>();
			for (int i = 0; i < TOTALVERSIONS; i++) {
				// system.out.println("Query at version " + i);

				Query query = QueryFactory.create(queryString);
				long startTime = System.currentTimeMillis();

				if (true || !rol.equalsIgnoreCase("SPO"))
					solutions.put(i, materializeQuery(i, query));
				else
					solutions.put(i, materializeASKQuery(i, query));

				long endTime = System.currentTimeMillis();
				// system.out.println("Time:" + (endTime - startTime));
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
						+ ent.getValue().getStandardDeviation() + " " + ent.getValue().getN()+ " "+ent.getValue().getSum());
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
	public ArrayList<String> getQueriesWithResults(String queryFile, String rol) throws FileNotFoundException, IOException,
			InterruptedException, ExecutionException {
		ArrayList<String>  ret = new ArrayList<String> ();
		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";


		while ((line = br.readLine()) != null) {
			String[] parts = line.split(" ");
			// String element = parts[0]; //we take all parts in order to process all TP patterns

			/*
			 * warmup the system
			 */
			warmup();

			String queryString = QueryUtils.createLookupQuery(rol, parts);
			//System.out.println("queryString:" + queryString);
			Boolean hasSolution=false;
			int i=0;
			while (i<TOTALVERSIONS && !hasSolution){
				// system.out.println("Query at version " + i);

				Query query = QueryFactory.create(queryString);

				ArrayList<String> sols;
				if (true || !rol.equalsIgnoreCase("SPO")){
					sols = materializeQuery(i, query);
				}
				else
					sols= materializeASKQuery(i, query);

				if (sols!=null && sols.size()>0){
					hasSolution=true;
				}
				i++;

			}
			if (hasSolution)
				ret.add(line);

		}
		br.close();

		return ret;
	}

	private ArrayList<String> materializeQuery(int version, Query query) throws InterruptedException, ExecutionException {

		ArrayList<String> ret = new ArrayList<String>();

		QueryExecution qexec = QueryExecutionFactory.create(query, datasets.get(version));
		ResultSet results = qexec.execSelect();

		//System.out.println(query);
		// System.out.println("Version:" + version);

		while (results.hasNext()) {
			QuerySolution soln = results.next();
			String rowResult = QueryUtils.serializeSolution(soln);
			ret.add(rowResult);
			// System.out.println("****** RowResult:: " + rowResult);
		}

		return ret;
	}

	public ArrayList<String> materializeASKQuery(int version, Query query) throws InterruptedException, ExecutionException {

		ArrayList<String> ret = new ArrayList<String>();

		QueryExecution qexec = QueryExecutionFactory.create(query, datasets.get(version));
		Boolean result = qexec.execAsk();

		// System.out.println("Version:" + version);

		ret.add(result.toString());

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

		ArrayList<String> finalAdds = new ArrayList<String>();
		ArrayList<String> finalDels = new ArrayList<String>();

		Query query = QueryFactory.create(queryString);
		long startTime = System.currentTimeMillis();

		/**
		 * START PARALELL
		 */

		Collection<Callable<QueryResult>> tasks = new ArrayList<Callable<QueryResult>>();
		// // for the (initial version +1) up to the post version
		// // Note that it is +1 in order to compute the difference with the
		// // following one

		tasks.add(new TaskCallable(query, datasets.get(startVersionQuery), startVersionQuery, true));
		tasks.add(new TaskCallable(query, datasets.get(endVersionQuery), endVersionQuery, true));
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
					String rowResult = QueryUtils.serializeSolution(soln);

					// system.out.println("****** RowResult finalResultsStart: " + rowResult);
					finalResultsStart.add(rowResult);
				}
			} else {
				while (res.getSol().hasNext()) {
					QuerySolution soln = res.getSol().next();
					String rowResult = QueryUtils.serializeSolution(soln);

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
		// printStreamTime.println(queryFile + "," + (endTime - startTime));

		// for (int i=0;i<TOTALVERSIONS;i++){
		// qexec_add[i].close();
		// qexec_del[i].close();
		// }

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

		DescriptiveStatistics total = new DescriptiveStatistics();

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
				int versionQuery = index;
				ArrayList<String> finalAdds = new ArrayList<String>();
				ArrayList<String> finalDels = new ArrayList<String>();

				int postversionQuery = versionQuery + 1;
				if (jump > 0) {
					postversionQuery = Math.min((index + 1) * jump, TOTALVERSIONS - 1);
					versionQuery = 0;
				}
				// system.out.println("versionQuery:" + versionQuery + " ; postQuery:" + postversionQuery);

				String queryString = QueryUtils.createLookupQuery(rol, parts);
				//System.out.println("queryString:" + queryString);
				Query query = QueryFactory.create(queryString);

				long startTime = System.currentTimeMillis();

				/**
				 * START PARALELL
				 */

				TreeMap<Integer, QueryResult> resultStart = new TreeMap<Integer, QueryResult>();
				TreeMap<Integer, QueryResult> resultEnd = new TreeMap<Integer, QueryResult>();

				Set<TaskThread> a = new HashSet<TaskThread>();

				Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;

				TaskThread taskStart = new TaskThread(query, datasets.get(versionQuery), versionQuery, resultStart, askQuery);
				TaskThread taskEnd = new TaskThread(query, datasets.get(postversionQuery), postversionQuery, resultEnd, askQuery);
				taskStart.start();
				a.add(taskStart);
				taskEnd.start();
				a.add(taskEnd);

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

				HashSet<String> finalResultsStart = new HashSet<String>();
				HashSet<String> finalResultsEnd = new HashSet<String>();
				if (!askQuery) {
					while (resultStart.get(versionQuery).getSol().hasNext()) {
						QuerySolution soln = resultStart.get(versionQuery).getSol().next();
						String rowResult = QueryUtils.serializeSolution(soln);

						finalResultsStart.add(rowResult);
						//System.out.println("start:" + rowResult);

					}
					while (resultEnd.get(postversionQuery).getSol().hasNext()) {
						QuerySolution soln = resultEnd.get(postversionQuery).getSol().next();
						String rowResult = QueryUtils.serializeSolution(soln);

						finalResultsEnd.add(rowResult);
						//System.out.println("end:" + rowResult);

					}
				} else {
					finalResultsStart.add(resultStart.get(versionQuery).getSolAsk().toString());
					finalResultsEnd.add(resultEnd.get(postversionQuery).getSolAsk().toString());
				}

				Iterator<String> it = finalResultsStart.iterator();
				String res;
				while (it.hasNext()) {
					res = it.next();
					if (!finalResultsEnd.contains(res)) {
						// System.out.println("del:" + res);
						finalDels.add(res);
					}
					// element is the response
					// printStream.println(">" + res);
					// System.out.println(">" + res);
					// System.out.println("count:" + count);
				}
				it = finalResultsEnd.iterator();
				while (it.hasNext()) {
					res = it.next();
					if (!finalResultsStart.contains(res)) {
						// System.out.println("add:" + res);
						finalAdds.add(res);
					}
					// element is the response
					// printStream.println("<" + res);
					// System.out.println("<" + res);
					// System.out.println("count:" + count);
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
						+ ent.getValue().getStandardDeviation() + " " + ent.getValue().getN()+ " "+ent.getValue().getSum());
			}
			pw.println("tot," + total.getMin() + "," + total.getMean() + "," + total.getMax() + "," + total.getStandardDeviation() + ","
					+ total.getN());
			pw.close();
		}
		br.close();
		return ret;
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

		/**
		 * START PARALELL
		 */

		Collection<Callable<QueryResult>> tasks = new ArrayList<Callable<QueryResult>>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			tasks.add(new TaskCallable(query, datasets.get(i), i, true));
		}
		ExecutorService executor = Executors.newFixedThreadPool(TOTALVERSIONS);
		List<Future<QueryResult>> results = executor.invokeAll(tasks);

		/**
		 * END PARALELL
		 */

		for (Future<QueryResult> result : results) {
			QueryResult res = result.get();
			// system.out.println("version:" + res.version);
			// system.out.println("version:" + res.sol.hasNext());
			ArrayList<String> solutions = new ArrayList<String>();
			while (res.getSol().hasNext()) {
				QuerySolution soln = res.getSol().next();
				String rowResult = QueryUtils.serializeSolution(soln);
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
		// for (int i=0;i<TOTALVERSIONS;i++){
		// qexec_add[i].close();
		// qexec_del[i].close();
		// }
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
			Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;
			Collection<Callable<QueryResult>> tasks = new ArrayList<Callable<QueryResult>>();
			for (int i = 0; i < TOTALVERSIONS; i++) {
				tasks.add(new TaskCallable(query, datasets.get(i), i, true, askQuery));
			}
			ExecutorService executor = Executors.newFixedThreadPool(TOTALVERSIONS);
			List<Future<QueryResult>> results = executor.invokeAll(tasks);

			/**
			 * END PARALELL
			 */

			for (Future<QueryResult> result : results) {
				ArrayList<String> solutions = new ArrayList<String>();
				QueryResult res = result.get();
				// system.out.println("version:" + res.version);
				// System.out.println("version:" + res.sol.hasNext());
				if (!askQuery) {
					while (res.getSol().hasNext()) {
						QuerySolution soln = res.getSol().next();
						String rowResult = QueryUtils.serializeSolution(soln);
						// system.out.println("rowResult:" + rowResult);
						solutions.add(rowResult);
						// rowResult is the final result for version res.version
					}
				}
				else{
					solutions.add(res.getSolAsk().toString());
				}
				AllSolutions.put(res.getVersion(), solutions);
			}
			ret.add(AllSolutions);

			executor.shutdown(); // always reclaim resources

			long endTime = System.currentTimeMillis();
			// system.out.println("Time:" + (endTime - startTime));
			total.addValue((endTime - startTime));

			// printStreamTime.println(queryFile + "," + (endTime - startTime));

		}
		br.close();
		if (measureTime) {
			// PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-dynver-" + inputFile.getName()));
			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("##name, min, mean, max, stddev, count, total");
			pw.println("tot," + total.getMin() + "," + total.getMean() + "," + total.getMax() + "," + total.getStandardDeviation() + ","
					+ total.getN()+ " "+total.getSum());
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

		TreeMap<Integer, QueryResult> results = new TreeMap<Integer, QueryResult>();

		Set<TaskThread> a = new HashSet<TaskThread>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			TaskThread task = new TaskThread(query, datasets.get(i), i, results);
			task.start();
			a.add(task);
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
			while (results.get(i).getSol().hasNext()) {
				QuerySolution soln = results.get(i).getSol().next();
				// System.out.println("++ ADDED in Version:" + i);
				String rowResult = QueryUtils.serializeSolution(soln);

				// System.out.println("****** RowResult: " + rowResult);
				finalResults.add(rowResult);
			}
		}

	}

	private static String createWarmupQuery() {
		String queryString = "SELECT ?element1 ?element2 ?element3 WHERE { " + " ?element1 ?element2 ?element3 ."

		+ "}" + "LIMIT 100";

		return queryString;
	}

	/**
	 * @param queryFile
	 * @param dataset_adds
	 * @param dataset_dels
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	// TODO Review, Not sure if this is implemented correctly in CB
	private void bulkAllChangeQuerying(String queryFile, String rol) throws InterruptedException, ExecutionException, IOException {

	}

	/**
	 * close Jena TDB and release resources
	 * 
	 * @param directory
	 * @throws RuntimeException
	 */
	public void close() throws RuntimeException {
		for (int i = 0; i < TOTALVERSIONS; i++) {
			datasets.get(i).end();
		}

	}
}
