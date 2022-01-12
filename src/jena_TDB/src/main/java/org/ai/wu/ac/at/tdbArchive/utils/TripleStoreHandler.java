package org.ai.wu.ac.at.tdbArchive.utils;

import org.ai.wu.ac.at.tdbArchive.api.JenaTDBArchive.TripleStore;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_TB;
import org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.riot.Lang;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDBLoader;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb.setup.DatasetBuilderStd;
import org.apache.jena.tdb.store.DatasetGraphTDB;
import org.apache.jena.util.FileManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfig;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryConfigSchema;
import org.openrdf.repository.manager.LocalRepositoryManager;
import org.openrdf.repository.manager.RepositoryManager;
import org.openrdf.rio.*;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.openrdf.rio.helpers.StatementCollector;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class TripleStoreHandler {
    private static final Logger logger = LogManager.getLogger(TripleStoreHandler.class);

    private String endpoint;
    private long ingestionTime;
    private String tripleStoreLoc;
    private FusekiServer fusekiServer;
    private TripleStore tripleStore;

    public String getEndpoint() {
        return this.endpoint;
    }

    public long getIngestionTime() {
        return this.ingestionTime;
    }

    public String getTripleStoreLoc() {
        return this.tripleStoreLoc;
    }

    public void load(String directory, String format, TripleStore tripleStore) {
        if (tripleStore == TripleStore.JenaTDB) {
            Lang rdf_format = null;
            switch (format) {
                case "nq": rdf_format = Lang.NQ;
                case "ttl": rdf_format = Lang.TTL;
            }
            logger.info("Initializing Jena Fuseki Server");
            this.tripleStore = TripleStore.JenaTDB;

            // Initialize Jena
            ARQ.init();

            // Create a TDB persistent dataset in tmp/TDB/currentTimestamp and load the .nq file into it.
            this.tripleStoreLoc = "target/TDB";
            DatasetGraphTDB dsg = DatasetBuilderStd.create(Location.create(this.tripleStoreLoc));
            logger.info(String.format("If you are using docker the TDB dataset will be located " +
                    "in /var/lib/docker/overlay2/<buildID>/diff/%s", this.tripleStoreLoc));
            FileManager fm = FileManager.get();
            fm.addLocatorClassLoader(JenaTDBArchive_query.class.getClassLoader());
            InputStream in = fm.open(directory);
            long startTime = System.currentTimeMillis();
            TDBLoader.load(dsg, in, rdf_format,false, true);
            long endTime = System.currentTimeMillis();
            logger.info("Loaded in "+(endTime - startTime)/1000 +" seconds");
            this.ingestionTime = (endTime - startTime)/1000;

            // Create a dataset object from the persistent TDB dataset
            Dataset dataset = TDBFactory.createDataset(this.tripleStoreLoc);
            // Create a fuseki server, load the dataset into the repository
            // http://localhost:3030/in_memory_server/sparql and connect to it.
            this.fusekiServer = FusekiServer.create()
                    .add("/in_memory_server", dataset)
                    .build();
            fusekiServer.start();
            this.endpoint = String.format("http://localhost:%d/in_memory_server/sparql", fusekiServer.getHttpPort());
            logger.info("Jena Fuseki Server endpoint: " + this.endpoint);
            dataset.end();

        }
        else if (tripleStore == TripleStore.GraphDB) {
            /*
            // Instantiate a local repository manager and initialize it
            RepositoryManager repositoryManager = new LocalRepositoryManager(new File("."));
            repositoryManager.initialize();

            // Instantiate a repository graph model
            TreeModel graph = new TreeModel();

            // Read repository configuration file
            FileManager fm = FileManager.get();
            fm.addLocatorClassLoader(JenaTDBArchive_query.class.getClassLoader());
            InputStream config = fm.open("/repo-defaults.ttl");
            //InputStream config = EmbeddedGraphDB.class.getResourceAsStream("/repo-defaults.ttl");
            RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
            rdfParser.setRDFHandler(new RDFHandlerBase() {
                @Override
                public void handleStatement(Statement st)
                        throws RDFHandlerException {
                    try {
                        conn.add(st);
                    } catch (OpenRDFException e) {
                        throw new RDFHandlerException(e);
                    }
                }
            });
            rdfParser.parse(getClass().getResourceAsStream("TestTicket276.n3"), "");
            rdfParser.parse(config, RepositoryConfigSchema.NAMESPACE);
            config.close();

            // Retrieve the repository node as a resource
            Resource repositoryNode = GraphUtil.getUniqueSubject(graph, RDF.TYPE, RepositoryConfigSchema.REPOSITORY);

            // Create a repository configuration object and add it to the repositoryManager
            RepositoryConfig repositoryConfig = RepositoryConfig.create(graph, repositoryNode);
            repositoryManager.addRepositoryConfig(repositoryConfig);

            // Get the repository from repository manager, note the repository id set in configuration .ttl file
            Repository repository = repositoryManager.getRepository("graphdb-repo");
            */

        }

    }

    private void connect() {
        // Open a connection to this repository
        //RepositoryConnection repositoryConnection = repository.getConnection();
    }

    public void shutdown() {
        if (this.tripleStore == TripleStore.JenaTDB) {
            fusekiServer.stop();
        }
        else if (tripleStore == TripleStore.GraphDB) {
            //repositoryConnection.close();
            //repository.shutDown();
            //repositoryManager.shutDown();
        }
    }
}
