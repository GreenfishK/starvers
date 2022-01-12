package org.ai.wu.ac.at.tdbArchive.core;

import org.ai.wu.ac.at.tdbArchive.api.JenaTDBArchive;
import org.ai.wu.ac.at.tdbArchive.solutions.DiffSolution;
import org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query;
import org.ai.wu.ac.at.tdbArchive.utils.QueryUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.*;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.riot.Lang;
import org.apache.jena.sparql.mgt.Explain;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDBLoader;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb.setup.DatasetBuilderStd;
import org.apache.jena.tdb.store.DatasetGraphTDB;
import org.apache.jena.util.FileManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;

public class JenaTDBArchive_TB_star_f implements JenaTDBArchive {
	private static final Logger logger = LogManager.getLogger(JenaTDBArchive_TB_star_f.class);

	private int TOTALVERSIONS = 0;
	private String initialVersionTS;
	private String outputTime = "timeApp.txt";
	private Boolean measureTime = false;
	private FusekiServer server;
	private RDFConnection conn;
	private int tdbDatasetSize;
	private int cntTriples;

	// private static String metadataVersions = "<http://example.org/isVersion>";

	/**
	 * @param outputTime
	 */
	public void setOutputTime(String outputTime) {
		this.outputTime = outputTime;
		this.measureTime = true;
	}

	public JenaTDBArchive_TB_star_f() throws FileNotFoundException {
		this.measureTime = false;
	}

	public int getTdbDatasetSize() {
		return this.tdbDatasetSize;
	}

	public int getCntTriples() {
		return this.cntTriples;
	}

	/**
	 * Load Jena TDB from directory
	 * 
	 * @param directory The directory of multiple rdf files
	 * 	 * or location of a single rdf file (e.g. ttl or nq).
	 */
	public void load(String directory, TripleStore tripleStore) {
		// Initialize Jena
		ARQ.init();
		FileManager fm = FileManager.get();
		fm.addLocatorClassLoader(JenaTDBArchive_query.class.getClassLoader());

		//Create a TDB persistent dataset in tmp/TDB/currentTimestamp and load the .nq file into it.
		String currentTimestamp = String.valueOf(System.currentTimeMillis());
		String tdb_loc = "target/TDB"; // + currentTimestamp;
		DatasetGraphTDB dsg = DatasetBuilderStd.create(Location.create(tdb_loc));
		logger.info(String.format("If you are using docker the TDB dataset will be located " +
				"in /var/lib/docker/overlay2/<buildID>/diff/%s", tdb_loc));
		InputStream in = fm.open(directory);
		long startTime = System.currentTimeMillis();
		TDBLoader.load(dsg, in, Lang.TTL,false, true);
		long endTime = System.currentTimeMillis();
		logger.info("Loaded in "+(endTime - startTime)/1000 +" seconds");

		Dataset dataset;
		try {
			//Create a dataset object from the persistent TDB dataset
			dataset = TDBFactory.createDataset(tdb_loc);

			// Write dataset info file if the location of file where the query performances will be stored is given
			// Writes in the same directory as the query performance file
			if(!this.outputTime.equals("")) {
				File datasetLogFileDir = new File(this.outputTime).getParentFile();
				long tbdDirSize = FileUtils.sizeOfDirectory(new File(tdb_loc));
				long rawDataFileSize = FileUtils.sizeOf(new File(directory));

				logger.debug(datasetLogFileDir);
				String datasetLogFile = datasetLogFileDir + "/dataset_infos.csv";
				logger.debug(datasetLogFile);
				File f = new File(datasetLogFile);
				PrintWriter pw;
				if ( f.exists() && !f.isDirectory() ) {
					pw = new PrintWriter(new FileOutputStream(datasetLogFile, true));
				}
				else {
					pw = new PrintWriter(datasetLogFile);
					pw.append("ds_name,rdf_store_name,raw_data_size_in_MB,triple_store_size_in_MB,ingestion_time_in_s\n");
				}
				pw.append("bearb_jena_tdb_tb_star_f" +  "," + "Jena TDB" + "," + rawDataFileSize/1000000
						+ "," + tbdDirSize/1000000  + "," + (endTime - startTime)/1000 +"\n");
				pw.close();
				logger.info(String.format("Writing dataset logs to directory: %s", datasetLogFile));
			}

			//Create a fuseki server, load the dataset into the repository
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

		//Get the number of versions in the dataset (number of named graphs) and the initial version timestamp
		QueryExecution qExec = conn.query(QueryUtils.getVersionInfos_f());
		ResultSet results = qExec.execSelect();
		logger.info("Results from load query: " + results);
		while (results.hasNext()) {
			QuerySolution soln = results.next();
			int cntVersions = soln.getLiteral("cnt_versions").getInt();
			logger.info("Number of distinct versions:" + cntVersions); //INFO
			String initVersionTS = soln.getLiteral("initial_version_ts").getString();
			logger.info("Initial version timestamp:" + initVersionTS); //INFO

			this.TOTALVERSIONS = cntVersions;
			this.initialVersionTS = initVersionTS;
		}
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
        //TODO: implement, if necessary 
		return null;
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
     	//TODO: implement, if necessary 
		return null;
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
     	//TODO: implement, if necessary 
		return null;
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
     	//TODO: implement, if necessary 
		return null;
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
		warmup();

		ArrayList<Map<Integer, ArrayList<String>>> ret = new ArrayList<Map<Integer, ArrayList<String>>>();
		File inputFile = new File(queryFile);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		String line = "";

		TreeMap<Integer, DescriptiveStatistics> vStats = new TreeMap<Integer, DescriptiveStatistics>();
		for (int i = 0; i < TOTALVERSIONS; i++) {
			vStats.put(i, new DescriptiveStatistics());
		}
		Boolean askQuery = rol.equalsIgnoreCase("SPO") && false;
		DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSXXXXX");
		OffsetDateTime version_ts = OffsetDateTime.parse(this.initialVersionTS, DATE_TIME_FORMATTER);

		for (int lines = 0; (line = br.readLine()) != null; lines++) {
			String[] parts = line.split(" ");

			Map<Integer, ArrayList<String>> solutions = new HashMap<Integer, ArrayList<String>>();
			logger.info(String.format("Query %x%n", lines+1));
			for (int i = 0; i < TOTALVERSIONS; i++) {
				String queryString = QueryUtils.createLookupQueryRDFStar_f(rol, parts, version_ts.toString());
				int limit = QueryUtils.getLimit(parts);
				Query query = QueryFactory.create(queryString);

				long startTime = System.currentTimeMillis();
				if (true || !askQuery)
					solutions.put(i, materializeQuery(i, query, limit));
				else
					solutions.put(i, materializeASKQuery(i, query));
				long endTime = System.currentTimeMillis();

				vStats.get(i).addValue((endTime - startTime));
				version_ts = version_ts.plusSeconds(1);
			}
			ret.add(solutions);
		}
		br.close();

		if (measureTime) {
			// PrintWriter pw = new PrintWriter(new File(outputDIR + "/res-dynmat-" + inputFile.getName()));
			PrintWriter pw = new PrintWriter(new File(outputTime));
			pw.println("ver, min, mean, max, stddev, count, sum");
			for (Entry<Integer, DescriptiveStatistics> ent : vStats.entrySet()) {
				pw.println(ent.getKey() + ", " + ent.getValue().getMin() + ", " + ent.getValue().getMean()
						+ ", " + ent.getValue().getMax() + ", " + ent.getValue().getStandardDeviation()
						+ ", " + ent.getValue().getN()+", "+ent.getValue().getSum());
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
	private ArrayList<String> materializeQuery(int staticVersionQuery, Query query, int limit)
			throws InterruptedException, ExecutionException {
		conn = RDFConnection.connect(String.format("http://localhost:%d/in_memory_server/sparql", server.getHttpPort()));
		logger.info(String.format("Executing version %d", staticVersionQuery));
		QueryExecution qExec = conn.query(query.toString());
		logger.info(query.toString());
		ResultSet results = qExec.execSelect();
		Boolean higherVersion = false;

		ArrayList<String> ret = new ArrayList<String>();
		while (results.hasNext() && !higherVersion && limit-- > 0) {
			QuerySolution soln = results.next();
			String rowResult = QueryUtils.serializeSolutionFilterOutGraphs(soln);
			ret.add(rowResult);
		}
		qExec.close();
		conn.close();
		return ret;
	}

	/**
	 * @param staticVersionQuery
	 * @param query
	 * @return
	 */
	private ArrayList<String> materializeASKQuery(int staticVersionQuery, Query query) throws InterruptedException, ExecutionException {
     	//TODO: implement, if necessary 
		return null;
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
     	//TODO: implement, if necessary 
		return null;
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
     	//TODO: implement, if necessary 
		return null;
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
		QueryExecution qExec = conn.query(createWarmupQuery());
		ResultSet results = qExec.execSelect();
		long endTime = System.currentTimeMillis();

		HashSet<String> finalResults = new HashSet<>();
		while (results.hasNext()) {
			QuerySolution soln = results.next();
			String rowResult = QueryUtils.serializeSolution(soln);
			finalResults.add(rowResult);
		}
		logger.info("Warmup Time:" + (endTime - startTime));
		logger.info(finalResults);
		qExec.close();
		conn.close();
	}

	private static String createWarmupQuery() {
		return "select ?s ?p ?o where { <<?s ?p ?o>> ?x ?y . } limit 100";
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
