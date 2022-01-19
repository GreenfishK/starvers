package org.ai.wu.ac.at.tdbArchive.core;

import org.ai.wu.ac.at.tdbArchive.api.RDFArchive;
import org.ai.wu.ac.at.tdbArchive.api.RDFStarAnnotationStyle;
import org.ai.wu.ac.at.tdbArchive.api.TripleStore;
import org.ai.wu.ac.at.tdbArchive.solutions.DiffSolution;
import org.ai.wu.ac.at.tdbArchive.utils.DatasetUtils;
import org.ai.wu.ac.at.tdbArchive.utils.QueryUtils;
import org.ai.wu.ac.at.tdbArchive.utils.TripleStoreHandler;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import java.io.*;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;


public class GraphDBArchive_TB_star implements RDFArchive {
    private static final Logger logger = LogManager.getLogger(GraphDBArchive_TB_star.class);

    private int TOTALVERSIONS = 0;
    private String initialVersionTS;
    private String outputTime = "timeApp.txt";
    private TripleStoreHandler ts;
    private Boolean measureTime = false;
    private final RDFStarAnnotationStyle annotationStyle;

    // private static String metadataVersions = "<http://example.org/isVersion>";

    /**
     * @param outputTime
     */
    public void setOutputTime(String outputTime) {
        this.outputTime = outputTime;
        this.measureTime = true;
    }

    public GraphDBArchive_TB_star(RDFStarAnnotationStyle annotationStyle) {
        this.measureTime = false;
        this.annotationStyle = annotationStyle;
    }

    /**
     * Load Graph DB from directory
     *
     * @param directory The directory of multiple rdf files
     * 	 * or location of a single rdf file (e.g. ttl or nq).
     */
    public void load(String directory) {
        this.ts = new TripleStoreHandler();
        ts.load(directory, "ttl", TripleStore.GraphDB);

        if(!this.outputTime.equals("")) {
            try {
                // Write dataset info file if the location of file where the query performances will be stored is given
                // Writes in the same directory as the query performance file
                DatasetUtils dsUtils = new DatasetUtils();
                dsUtils.logDatasetInfos(TripleStore.GraphDB, ts.getIngestionTime(), ts.getTripleStoreLoc(),
                        directory, this.outputTime);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        //Get the number of versions in the dataset (number of named graphs) and the initial version timestamp
        String sparqlEndpoint = this.ts.getEndpoint();
        // TODO use sparql Endpoint for connection to GraphDB

        try (RepositoryConnection conn = ts.getGraphDBConnection()) {
            String query = null;
            if (annotationStyle == RDFStarAnnotationStyle.FLAT) {
                query = QueryUtils.getVersionInfos_f();
            } else if (annotationStyle == RDFStarAnnotationStyle.HIERARCHICAL) {
                query = QueryUtils.getVersionInfos_h();
            } else {
                logger.error("The Annotation Style must either be FLAT or HIERARCHICAL. Query is set to null");
            }
            TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);

            TupleQueryResult tupleQueryResult = tupleQuery.evaluate();
            while (tupleQueryResult.hasNext()) {
                BindingSet bindingSet = tupleQueryResult.next();
                this.TOTALVERSIONS = Integer.parseInt(bindingSet.getBinding("cnt_versions").getValue().stringValue());
                this.initialVersionTS = bindingSet.getBinding("initial_version_ts").getValue().stringValue();
            }
            tupleQueryResult.close();
        }

        logger.info("Number of distinct versions:" + this.TOTALVERSIONS);
        logger.info("Initial version timestamp:" + this.initialVersionTS);
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
        boolean askQuery = rol.equalsIgnoreCase("SPO") && false;
        DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSXXXXX");
        OffsetDateTime version_ts = OffsetDateTime.parse(this.initialVersionTS, DATE_TIME_FORMATTER);

        for (int lines = 0; (line = br.readLine()) != null; lines++) {
            String[] parts = line.split(" ");

            Map<Integer, ArrayList<String>> solutions = new HashMap<Integer, ArrayList<String>>();
            logger.info(String.format("Query %x%n", lines+1));
            for (int i = 0; i < TOTALVERSIONS; i++) {
                String queryString = null;
                if (annotationStyle == RDFStarAnnotationStyle.FLAT) {
                    queryString = QueryUtils.createLookupQueryRDFStar_f(rol, parts, version_ts.toString());
                } else if (annotationStyle == RDFStarAnnotationStyle.HIERARCHICAL) {
                    queryString = QueryUtils.createLookupQueryRDFStar_h(rol, parts, version_ts.toString());
                } else {
                    logger.error("The Annotation Style must either be FLAT or HIERARCHICAL. queryString is set to null");
                }
                int limit = QueryUtils.getLimit(parts);

                long startTime = System.currentTimeMillis();
                if (true || !askQuery)
                    solutions.put(i, materializeQuery(i, queryString, limit));
                else
                    solutions.put(i, materializeASKQuery(i, queryString));
                long endTime = System.currentTimeMillis();

                vStats.get(i).addValue((endTime - startTime));
                version_ts = version_ts.plusSeconds(1);
            }
            ret.add(solutions);
        }
        br.close();

        if (measureTime)
            QueryUtils.logQueryStatistics(TripleStore.GraphDB, outputTime, vStats);

        return ret;
    }

    /**
     * @param staticVersionQuery
     * @param query
     * @return
     */
    private ArrayList<String> materializeQuery(int staticVersionQuery, String query, int limit)
            throws InterruptedException, ExecutionException {
        boolean higherVersion = false;
        ArrayList<String> ret = new ArrayList<String>();

        try (RepositoryConnection conn = ts.getGraphDBConnection()) {
            logger.info(String.format("Executing version %d", staticVersionQuery));
            TupleQuery qExec = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            TupleQueryResult results = qExec.evaluate();

            while (results.hasNext() && !higherVersion && limit-- > 0) {
                BindingSet bindingSet = results.next();
                String rowResult = QueryUtils.serializeSolution(bindingSet);
                ret.add(rowResult);
            }
            results.close();
        }
        return ret;
    }

    /**
     * @param staticVersionQuery
     * @param query
     * @return
     */
    private ArrayList<String> materializeASKQuery(int staticVersionQuery, String query) throws InterruptedException, ExecutionException {
        //TODO: implement, if necessary
        return null;
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

        HashSet<String> finalResults = new HashSet<>();
        long startTime = 0;
        long endTime = 0;
        String sparqlEndpoint = this.ts.getEndpoint();
        //TODO: use sparql endpoint to connect to graphDB

        try (RepositoryConnection conn = ts.getGraphDBConnection()) {
            TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL,createWarmupQuery());

            startTime = System.currentTimeMillis();
            TupleQueryResult tupleQueryResult = tupleQuery.evaluate();
            endTime = System.currentTimeMillis();

            while (tupleQueryResult.hasNext()) {
                BindingSet bindingSet = tupleQueryResult.next();
                String rowResult = QueryUtils.serializeSolution(bindingSet);
                finalResults.add(rowResult);
            }
            tupleQueryResult.close();
        }

        logger.info("Warmup Time:" + (endTime - startTime));
        logger.info("Query results: " + finalResults);

    }

    private String createWarmupQuery() {
        if (annotationStyle == RDFStarAnnotationStyle.HIERARCHICAL)
            return "select ?s ?p ?o where { <<<<?s ?p ?o>> ?x ?y>> ?a ?b . } limit 100";
        else if (annotationStyle == RDFStarAnnotationStyle.FLAT)
            return "select ?s ?p ?o where { <<?s ?p ?o>> ?x ?y . } limit 100";
        else {
            logger.error("The Annotation Style must either be FLAT or HIERARCHICAL. Query is set to null");
            return null;
        }
    }
    /**
     * close Graph DB and release resources
     *
     * @throws RuntimeException
     */
    public void close() throws RuntimeException {
        this.ts.shutdown();
    }
}
