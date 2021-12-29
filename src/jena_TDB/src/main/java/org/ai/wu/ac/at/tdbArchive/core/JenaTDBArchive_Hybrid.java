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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.ai.wu.ac.at.tdbArchive.api.JenaTDBArchive;
import org.ai.wu.ac.at.tdbArchive.solutions.DiffSolution;
import org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query;
import org.ai.wu.ac.at.tdbArchive.utils.QueryResult;
import org.ai.wu.ac.at.tdbArchive.utils.QueryUtils;
import org.ai.wu.ac.at.tdbArchive.utils.TaskThread;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jena.query.*;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.util.FileManager;

public class JenaTDBArchive_Hybrid implements JenaTDBArchive {

	private int TOTALVERSIONS = 0;
	private String outputTime = "timeApp.txt";
	private HashMap<Integer, Dataset> datasets = new HashMap<Integer, Dataset>(); // Version IC
	private HashMap<Integer, Dataset> datasets_adds = new HashMap<Integer, Dataset>(); // Version Diff, adds
	private HashMap<Integer, Dataset> datasets_dels = new HashMap<Integer, Dataset>(); // Version Diff, dels
	private TreeSet<Integer> materializedVersions = new TreeSet<Integer>(); // includes the versions with IC

	private Boolean measureTime = false;

	/**
	 * @param outputTime
	 */
	public void setOutputTime(String outputTime) {
		this.outputTime = outputTime;
		this.measureTime = true;
	}

	public JenaTDBArchive_Hybrid() throws FileNotFoundException {
		/*
		 * Load all datasets
		 */
		datasets = new HashMap<Integer, Dataset>(); // Version IC
		datasets_adds = new HashMap<Integer, Dataset>(); // Version Diff, adds
		datasets_dels = new HashMap<Integer, Dataset>(); // Version Diff, dels
		materializedVersions = new TreeSet<Integer>(); // includes the versions with IC
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
		// let's assume we have ic, and cb folders, and inside the versions 1 or 1/add, 1/del

		for (File fileEntry : folder.listFiles()) {
			if (fileEntry.getName().equalsIgnoreCase("ic")) {
				File subfolder = new File(directory + "/" + fileEntry.getName());
				for (File subfileEntry : subfolder.listFiles()) {
					int fileVersion = Integer.parseInt(subfileEntry.getName());
					// System.out.println("... Loading IC TDB version" + fileVersion);
					// System.out.println("Reading " + directory + "/" + fileEntry.getName() + "/" + fileVersion);
					datasets.put(fileVersion, TDBFactory.createDataset(directory + "/" + fileEntry.getName() + "/" + fileVersion));
					materializedVersions.add(fileVersion);
					TOTALVERSIONS++;
				}
			} else if (fileEntry.getName().equalsIgnoreCase("cb")) {
				File subfolder = new File(directory + "/" + fileEntry.getName());
				for (File subfileEntry : subfolder.listFiles()) {
					int fileVersion = Integer.parseInt(subfileEntry.getName());
					// System.out.println("... Loading CB add TDB version" + fileVersion);
					datasets_adds.put(fileVersion, TDBFactory.createDataset(directory + "/" + fileEntry.getName() + "/" + fileVersion + "/add/"));
					// System.out.println("... Loading CB del TDB version" + fileVersion);
					datasets_dels.put(fileVersion, TDBFactory.createDataset(directory + "/" + fileEntry.getName() + "/" + fileVersion + "/del/"));
					TOTALVERSIONS++;
				}
			}
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

		ArrayList<String> finalAdds = new ArrayList<String>();
		ArrayList<String> finalDels = new ArrayList<String>();

		Query query = QueryFactory.create(queryString);
		long startTime = System.currentTimeMillis();

		// System.out.println("versionQuery:" + startVersionQuery + " ; postQuery:" + endVersionQuery);

		HashSet<String> resultsStart = new HashSet<String>(materializeQuery(startVersionQuery, query));

		HashSet<String> resultsEnd = new HashSet<String>();
		// first check if staticVersionQuery is in IC
		if (datasets.get(endVersionQuery) != null) {
			// System.out.println("Version:" + endVersionQuery + " is in IC");
			resultsEnd = new HashSet<String>(materializeQuery(datasets.get(endVersionQuery), query));
		} else {
			// get closest IC less than the given version
			Integer lower = materializedVersions.lower(endVersionQuery);
			// System.out.println("Version:" + endVersionQuery + " is NOT in IC, it is CB");
			// get first results from IC and retrieve the Deltas

			// check if lower is the same as versionQuery
			if (lower == startVersionQuery) {
				resultsEnd = new HashSet<String>(resultsStart);
			}

			/**
			 * START PARALELL
			 */
			TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
			TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

			Set<TaskThread> a = new HashSet<TaskThread>();
			for (int i = lower + 1; i <= endVersionQuery; i++) {
				TaskThread task_add = new TaskThread(query, datasets_adds.get(i), i, true, results_adds);
				TaskThread task_del = new TaskThread(query, datasets_dels.get(i), i, false, results_dels);
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

			// add and del solution from IC
			for (int i = lower + 1; i <= endVersionQuery; i++) {
				// System.out.println("Iterating results " + i);
				while (results_adds.get(i).getSol().hasNext()) {
					QuerySolution soln = results_adds.get(i).getSol().next();
					// System.out.println("++ ADDED in Version:" + i);
					String rowResult = QueryUtils.serializeSolution(soln);

					resultsEnd.add(rowResult);
				}
				while (results_dels.get(i).getSol().hasNext()) {
					QuerySolution soln = results_dels.get(i).getSol().next();
					// System.out.println("-- DEL in Version:" + i);
					String rowResult = QueryUtils.serializeSolution(soln);

					resultsEnd.remove(rowResult);
				}
			}
		}

		// compute the deltas between result1 (initial) and results2 (end)
		for (String result : resultsStart) {
			if (!resultsEnd.contains(result)) {
				finalDels.add(result);
				// System.out.println("del:" + result);
			}
		}
		for (String result : resultsEnd) {
			if (!resultsStart.contains(result)) {
				finalAdds.add(result);
				// System.out.println("add:" + result);
			}
		}
		long endTime = System.currentTimeMillis();
		// System.out.println("Time:" + (endTime - startTime));
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
	 * @param queryFile
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws IOException
	 */
	private void bulkAllChangeQuerying(String queryFile, String rol, HashMap<Integer, Dataset> datasets, HashMap<Integer, Dataset> datasets_adds,
			HashMap<Integer, Dataset> datasets_dels, TreeSet<Integer> materializedVersions) throws InterruptedException, ExecutionException,
			IOException {

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

		Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;

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

				HashSet<String> resultsStart = new HashSet<String>();
				if (!askQuery)
					resultsStart = new HashSet<String>(materializeQuery(versionQuery, query));
				else
					resultsStart = new HashSet<String>(materializeASKQuery(versionQuery, query));
				HashSet<String> resultsEnd = new HashSet<String>();
				// first check if staticVersionQuery is in IC
				if (datasets.get(postversionQuery) != null) {
					// System.out.println("Version:" + postversionQuery + " is in IC");
					if (!askQuery)
						resultsEnd = new HashSet<String>(materializeQuery(datasets.get(postversionQuery), query));
					else
						resultsEnd = new HashSet<String>(materializeASKQuery(datasets.get(postversionQuery), query));
				} else {
					// get closest IC less than the given version
					Integer lower = materializedVersions.lower(postversionQuery);
					// System.out.println("Version:" + postversionQuery + " is NOT in IC, it is CB");
					// get first results from IC and retrieve the Deltas

					TreeMap<Integer, QueryResult> results = new TreeMap<Integer, QueryResult>();
					Set<TaskThread> a = new HashSet<TaskThread>();

					// check if lower is the same as versionQuery
					if (lower == versionQuery) {
						resultsEnd = new HashSet<String>(resultsStart);
					} else {
						TaskThread task = new TaskThread(query, datasets.get(lower), lower, true, results, askQuery);
						a.add(task);
						task.start();
					}

					/**
					 * START PARALELL
					 */
					TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
					TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

					for (int i = lower + 1; i <= postversionQuery; i++) {
						// System.out.println("getting adds and dels of version " + i);
						TaskThread task_add = new TaskThread(query, datasets_adds.get(i), i, true, results_adds, askQuery);
						TaskThread task_del = new TaskThread(query, datasets_dels.get(i), i, false, results_dels, askQuery);
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
					// get the IC if needed
					if (results != null && results.size() > 0) {
						if (!askQuery) {
							while (results.get(lower).getSol().hasNext()) {
								QuerySolution soln = results.get(lower).getSol().next();
								String rowResult = QueryUtils.serializeSolution(soln);
								resultsEnd.add(rowResult);
							}
						} else {
							resultsEnd.add(results.get(lower).getSolAsk().toString());
						}

					}

					// add and del solution from IC
					if (!askQuery) {
						for (int i = lower + 1; i <= postversionQuery; i++) {
							// System.out.println("Iterating results " + i);
							while (results_adds.get(i).getSol().hasNext()) {
								QuerySolution soln = results_adds.get(i).getSol().next();
								String rowResult = QueryUtils.serializeSolution(soln);
								// System.out.println("++ ADDED in Version:" + i+" ; "+rowResult);
								resultsEnd.add(rowResult);
							}
							while (results_dels.get(i).getSol().hasNext()) {
								QuerySolution soln = results_dels.get(i).getSol().next();
								String rowResult = QueryUtils.serializeSolution(soln);
								// System.out.println("-- DEL in Version:" + i+" ; "+rowResult);
								resultsEnd.remove(rowResult);
							}
						}
					} else {
						for (int i = lower + 1; i <= postversionQuery; i++) {
							resultsEnd.add(results_adds.get(i).getSolAsk().toString());
							resultsEnd.remove(results_dels.get(i).getSolAsk().toString());
						}
					}
				}

				// compute the deltas between result1 (initial) and results2 (end)

				for (String result : resultsStart) {
					if (!resultsEnd.contains(result)) {

						finalDels.add(result);
						// System.out.println("del:" + result);
					}
				}
				for (String result : resultsEnd) {
					if (!resultsStart.contains(result)) {

						finalAdds.add(result);
						// System.out.println("add:" + result);
					}
				}
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
			pw.println("##bucket, min, mean, max, stddev, count");
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

				long startTime = System.currentTimeMillis();
				if (!askQuery)
					solutions.put(i, materializeQuery(i, query));
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
			pw.println("##ver, min, mean, max, stddev, count,total");
			for (Entry<Integer, DescriptiveStatistics> ent : vStats.entrySet()) {
				pw.println(ent.getKey() + " " + ent.getValue().getMin() + " " + ent.getValue().getMean() + " " + ent.getValue().getMax() + " "
						+ ent.getValue().getStandardDeviation() + " " + ent.getValue().getN()+" "+ent.getValue().getSum());
			}
			pw.close();
		}
		return ret;
	}

	private ArrayList<String> materializeQuery(Dataset dataset, Query query) throws InterruptedException, ExecutionException {

		ArrayList<String> ret = new ArrayList<String>();
		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		ResultSet results = qexec.execSelect();

		while (results.hasNext()) {
			QuerySolution soln = results.next();
			String rowResult = QueryUtils.serializeSolution(soln);

			ret.add(rowResult);
		}
		return ret;
	}

	private ArrayList<String> materializeASKQuery(Dataset dataset, Query query) throws InterruptedException, ExecutionException {

		ArrayList<String> ret = new ArrayList<String>();

		QueryExecution qexec = QueryExecutionFactory.create(query, dataset);
		Boolean result = qexec.execAsk();

		// System.out.println("Version:" + version);

		ret.add(result.toString());

		return ret;
	}

	/**
	 * @param staticVersionQuery
	 * @param query
	 * @return
	 */
	private ArrayList<String> materializeQuery(int staticVersionQuery, Query query) throws InterruptedException, ExecutionException {

		HashSet<String> ret = new HashSet<String>();

		// first check if staticVersionQuery is in IC
		if (datasets.get(staticVersionQuery) != null) {
			// System.out.println("Version:" + staticVersionQuery + " is in IC");
			ret = new HashSet<String>(materializeQuery(datasets.get(staticVersionQuery), query));
		} else {
			// get closest IC less than the given version
			Integer lower = materializedVersions.lower(staticVersionQuery);
			// System.out.println("Version:" + staticVersionQuery + " is NOT in IC, it is CB");
			// System.out.println("lower IC is :" + lower);
			// get first results from IC and retrieve the Deltas

			/**
			 * START PARALELL
			 */
			TreeMap<Integer, QueryResult> results = new TreeMap<Integer, QueryResult>();
			TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
			TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

			Set<TaskThread> a = new HashSet<TaskThread>();
			TaskThread task = new TaskThread(query, datasets.get(lower), lower, true, results); // get first results from IC
			task.start();
			a.add(task);

			for (int i = lower + 1; i <= staticVersionQuery; i++) {
				TaskThread task_add = new TaskThread(query, datasets_adds.get(i), i, true, results_adds);
				TaskThread task_del = new TaskThread(query, datasets_dels.get(i), i, false, results_dels);
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
			// get solutions from IC
			while (results.get(lower).getSol().hasNext()) {
				QuerySolution soln = results.get(lower).getSol().next();
				// System.out.println("++ ADDED in Version:" + i);
				String rowResult = QueryUtils.serializeSolution(soln);
				ret.add(rowResult);
			}
			// add and del solution from IC
			for (int i = lower + 1; i <= staticVersionQuery; i++) {
				// System.out.println("Iterating results " + i);
				while (results_adds.get(i).getSol().hasNext()) {
					QuerySolution soln = results_adds.get(i).getSol().next();
					// System.out.println("++ ADDED in Version:" + i);
					String rowResult = QueryUtils.serializeSolution(soln);

					ret.add(rowResult);
				}
				while (results_dels.get(i).getSol().hasNext()) {
					QuerySolution soln = results_dels.get(i).getSol().next();
					// System.out.println("-- DEL in Version:" + i);
					String rowResult = QueryUtils.serializeSolution(soln);

					ret.remove(rowResult);
				}
			}
		}

		return new ArrayList<String>(ret);

	}

	/**
	 * @param staticVersionQuery
	 * @param query
	 * @return
	 */
	private ArrayList<String> materializeASKQuery(int staticVersionQuery, Query query) throws InterruptedException, ExecutionException {

		HashSet<String> ret = new HashSet<String>();

		// first check if staticVersionQuery is in IC
		if (datasets.get(staticVersionQuery) != null) {
			// System.out.println("Version:" + staticVersionQuery + " is in IC");
			ret = new HashSet<String>(materializeASKQuery(datasets.get(staticVersionQuery), query));
		} else {
			// get closest IC less than the given version
			Integer lower = materializedVersions.lower(staticVersionQuery);
			// System.out.println("Version:" + staticVersionQuery + " is NOT in IC, it is CB");
			// System.out.println("lower IC is :" + lower);
			// get first results from IC and retrieve the Deltas

			/**
			 * START PARALELL
			 */
			TreeMap<Integer, QueryResult> results = new TreeMap<Integer, QueryResult>();
			TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
			TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

			Set<TaskThread> a = new HashSet<TaskThread>();
			TaskThread task = new TaskThread(query, datasets.get(lower), lower, true, results, true); // get first results from IC
			task.start();
			a.add(task);

			for (int i = lower + 1; i <= staticVersionQuery; i++) {
				TaskThread task_add = new TaskThread(query, datasets_adds.get(i), i, true, results_adds, true);
				TaskThread task_del = new TaskThread(query, datasets_dels.get(i), i, false, results_dels, true);
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
			ret.add(results.get(lower).getSolAsk().toString());

			// add and del solution from IC
			for (int i = lower + 1; i <= staticVersionQuery; i++) {
				// System.out.println("Iterating results " + i);
				ret.add(results_adds.get(i).getSolAsk().toString());
				ret.remove(results_dels.get(i).getSolAsk().toString());
			}
		}

		return new ArrayList<String>(ret);

	}

	static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
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
		TreeMap<Integer, QueryResult> results = new TreeMap<Integer, QueryResult>();
		TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
		TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

		Set<TaskThread> a = new HashSet<TaskThread>();

		for (int i = 0; i < TOTALVERSIONS; i++) {
			if (datasets.get(i) != null) {
				TaskThread task = new TaskThread(query, datasets.get(i), i, true, results); // get first results from IC
				task.start();
				a.add(task);
			}
			if (datasets_adds.get(i) != null) {
				TaskThread task_add = new TaskThread(query, datasets_adds.get(i), i, true, results_adds);
				task_add.start();
				a.add(task_add);
			}
			if (datasets_dels.get(i) != null) {
				TaskThread task_del = new TaskThread(query, datasets_dels.get(i), i, true, results_dels);
				task_del.start();
				a.add(task_del);
			}
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

		ArrayList<String> finalResults = new ArrayList<String>();

		// for all versions
		for (int i = 0; i < TOTALVERSIONS; i++) {

			if (datasets.get(i) != null) {
				finalResults = new ArrayList<String>();
				// System.out.println("Version:" + i + " is in IC");

				while (results.get(i).getSol().hasNext()) {
					QuerySolution soln = results.get(i).getSol().next();
					String rowResult = QueryUtils.serializeSolution(soln);
					// System.out.println("result:" + rowResult);
					finalResults.add(rowResult);
				}
			} else {
				// System.out.println("Version:" + i + " is NOT in IC, it is CB");
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
			ret.put(i, new ArrayList<String>(finalResults));
		}

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
			TreeMap<Integer, QueryResult> results = new TreeMap<Integer, QueryResult>();
			TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
			TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

			Set<TaskThread> a = new HashSet<TaskThread>();

			for (int i = 0; i < TOTALVERSIONS; i++) {
				if (datasets.get(i) != null) {
					TaskThread task = new TaskThread(query, datasets.get(i), i, true, results, askQuery); // get first results from IC
					task.start();
					a.add(task);
				}
				if (datasets_adds.get(i) != null) {
					TaskThread task_add = new TaskThread(query, datasets_adds.get(i), i, true, results_adds, askQuery);
					task_add.start();
					a.add(task_add);
				}
				if (datasets_dels.get(i) != null) {
					TaskThread task_del = new TaskThread(query, datasets_dels.get(i), i, true, results_dels, askQuery);
					task_del.start();
					a.add(task_del);
				}
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

				if (datasets.get(i) != null) {
					finalResults = new HashSet<String>();
					// System.out.println("Version:" + i + " is in IC");
					if (!askQuery) {
						while (results.get(i).getSol().hasNext()) {
							QuerySolution soln = results.get(i).getSol().next();
							String rowResult = QueryUtils.serializeSolution(soln);
							// System.out.println("result:" + rowResult);
							finalResults.add(rowResult);
						}
					} else {
						finalResults.add(results.get(i).getSolAsk().toString());
					}
				} else {
					// System.out.println("Version:" + i + " is NOT in IC, it is CB");
					// System.out.println("computing results ADD for version " + i);
					if (!askQuery) {
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

				}
				AllSolutions.put(i, new ArrayList<String>(finalResults));
			}
			long endTime = System.currentTimeMillis();
			// System.out.println("Time:" + (endTime - startTime));
			total.addValue((endTime - startTime));

			ret.add(AllSolutions);

			// printStreamTime.println(queryFile + "," + (endTime - startTime));

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
		TreeMap<Integer, QueryResult> results = new TreeMap<Integer, QueryResult>();
		TreeMap<Integer, QueryResult> results_adds = new TreeMap<Integer, QueryResult>();
		TreeMap<Integer, QueryResult> results_dels = new TreeMap<Integer, QueryResult>();

		Set<TaskThread> a = new HashSet<TaskThread>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			if (datasets.get(i) != null) {
				TaskThread task = new TaskThread(query, datasets.get(i), i, true, results);
				task.start();
				a.add(task);
			}
			if (datasets_adds.get(i) != null) {
				TaskThread task_add = new TaskThread(query, datasets_adds.get(i), i, true, results_adds);
				task_add.start();
				a.add(task_add);
			}
			if (datasets_dels.get(i) != null) {
				TaskThread task_del = new TaskThread(query, datasets_dels.get(i), i, false, results_dels);
				task_del.start();
				a.add(task_del);
			}
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
			if (results.get(i) != null) {
				while (results.get(i).getSol().hasNext()) {
					QuerySolution soln = results.get(i).getSol().next();
					// System.out.println("++ ADDED in Version:" + i);
					String rowResult = QueryUtils.serializeSolution(soln);

					// System.out.println("****** RowResult: " + rowResult);
					finalResults.add(rowResult);
				}
			}
			if (results_adds.get(i) != null) {
				// System.out.println("Iterating results " + i);
				while (results_adds.get(i).getSol().hasNext()) {
					QuerySolution soln = results_adds.get(i).getSol().next();
					// System.out.println("++ ADDED in Version:" + i);
					String rowResult = QueryUtils.serializeSolution(soln);

					// System.out.println("****** RowResult: " + rowResult);
					finalResults.add(rowResult);
				}
			}
			if (results_dels.get(i) != null) {
				while (results_dels.get(i).getSol().hasNext()) {
					QuerySolution soln = results_dels.get(i).getSol().next();
					// System.out.println("-- DEL in Version:" + i);
					String rowResult = QueryUtils.serializeSolution(soln);

					// System.out.println("****** RowResult: " + rowResult);
					finalResults.remove(rowResult);
				}
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
			if (datasets.get(i) != null)
				datasets.get(i).end();
			if (datasets_adds.get(i) != null)
				datasets_adds.get(i).end();
			if (datasets_dels.get(i) != null)
				datasets_dels.get(i).end();
		}

	}
}
