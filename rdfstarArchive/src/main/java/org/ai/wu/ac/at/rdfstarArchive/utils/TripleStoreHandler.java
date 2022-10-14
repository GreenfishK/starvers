package org.ai.wu.ac.at.rdfstarArchive.utils;

import org.ai.wu.ac.at.rdfstarArchive.api.TripleStore;
import org.ai.wu.ac.at.rdfstarArchive.tools.RDFArchive_query;
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

import java.io.*;

import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.manager.LocalRepositoryManager;
import org.eclipse.rdf4j.rio.*;

import java.io.InputStream;

public class TripleStoreHandler {
    private static final Logger logger = LogManager.getLogger(TripleStoreHandler.class);

    private String endpoint;
    private long ingestionTime;
    private String tripleStoreLoc;
    private FusekiServer fusekiServer;
    private LocalRepositoryManager repositoryManager;
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

    public TripleStore getTripleStore() {
        return this.tripleStore;
    }

    public void load(String directory, String format, TripleStore tripleStore) {

        if (tripleStore == TripleStore.JenaTDB) {
            this.tripleStoreLoc = "target/TDB";
            this.tripleStore = TripleStore.JenaTDB;

            Lang rdf_format = null;
            switch (format) {
                case "nq": rdf_format = Lang.NQ; break;
                case "ttl": rdf_format = Lang.TTL; break;
            }

            // Initialize Jena
            ARQ.init();

            // Create a TDB persistent dataset in tmp/TDB/currentTimestamp and load the .nq file into it.
            DatasetGraphTDB dsg = DatasetBuilderStd.create(Location.create(this.tripleStoreLoc));
            logger.info(String.format("If you are using docker the TDB dataset will be located " +
                    "in /var/lib/docker/overlay2/<buildID>/diff/%s", this.tripleStoreLoc));
            FileManager fm = FileManager.get();
            fm.addLocatorClassLoader(RDFArchive_query.class.getClassLoader());
            InputStream in = fm.open(directory);

            long startTime = System.currentTimeMillis();
            TDBLoader.load(dsg, in, rdf_format, false, true);
            long endTime = System.currentTimeMillis();
            logger.info("Loaded in "+(endTime - startTime)/1000 +" seconds");
            this.ingestionTime = (endTime - startTime)/1000;

            // Create a dataset object from the persistent TDB dataset
            Dataset dataset = TDBFactory.createDataset(this.tripleStoreLoc);
            
            // Create a fuseki server, load the dataset object into the repository
            logger.info("Initializing Jena Fuseki Server");
            this.fusekiServer = FusekiServer.create()
                    .add("/evalJenaTDB", dataset)
                    .build();
            fusekiServer.start();
            this.endpoint = String.format("http://localhost:%d/evalJenaTDB/sparql", fusekiServer.getHttpPort());
            logger.info("Jena Fuseki Server endpoint: " + this.endpoint);
            dataset.end();

        }
        else if (tripleStore == TripleStore.GraphDB) {
            try {
                this.tripleStoreLoc = "target/GraphDB";
                this.tripleStore = TripleStore.GraphDB;
                this.endpoint = String.format("http://localhost:%d/repositories/evalGraphDB", 7200);

                RDFFormat rdf_format = null;
                switch (format) {
                    case "nq": rdf_format = RDFFormat.NQUADS; break;
                    case "ttl": rdf_format = RDFFormat.TURTLE; break;
                }

                File baseDir = new File("target","GraphDB");
                baseDir.mkdirs();
                repositoryManager = new LocalRepositoryManager(baseDir);
                repositoryManager.init();

                if (repositoryManager.hasRepositoryConfig("evalGraphDB")) {
                    throw new RuntimeException("Repository evalGraphDB already exists.");
                }

                InputStream config = TripleStoreHandler.class.getResourceAsStream("/repo-defaults.ttl");
                Model repo_config_graph = Rio.parse(config, "", RDFFormat.TURTLE);

                Resource repositoryNode = Models.subject(repo_config_graph.filter(null, RDF.TYPE, RepositoryConfigSchema.REPOSITORY)).orElse(null);
                repo_config_graph.add(repositoryNode, RepositoryConfigSchema.REPOSITORYID,
                        SimpleValueFactory.getInstance().createLiteral("evalGraphDB"));

                RepositoryConfig repositoryConfig = RepositoryConfig.create(repo_config_graph, repositoryNode);
                repositoryManager.addRepositoryConfig(repositoryConfig);

                //Load data
                RepositoryConnection conn = getGraphDBConnection();
                long startTime = System.currentTimeMillis();
                conn.add(new File(directory), rdf_format);
                long endTime = System.currentTimeMillis();
                logger.info("Loaded in "+(endTime - startTime)/1000 +" seconds");
                this.ingestionTime = (endTime - startTime)/1000;

                conn.commit();
                conn.close();

            } catch (RDFHandlerException | RDFParseException | IOException e) {
                logger.error("The GraphDB repository will not be created.");
                logger.error(e.getMessage());
                System.out.println(e.getMessage());
            }

        }
    }

    public RepositoryConnection getGraphDBConnection() {
        if (tripleStore == TripleStore.GraphDB) {
            try {
                return repositoryManager.getRepository("evalGraphDB").getConnection();
            } catch (RepositoryConfigException | RepositoryException e) {
                logger.error(e.getMessage());
                throw e;
            }
        } else {
            logger.error("To get a GraphDB connection the triple store must be GraphDB");
            return null;
        }
    }

    public RDFConnection getJenaTDBConnection() {
        if (tripleStore == TripleStore.JenaTDB) {
            return RDFConnection.connect(endpoint);
        } else {
            logger.error("To get a JenaTDB connection the triple store must be JenaTDB");
            return null;
        }
    }

    public void shutdown() {
        if (this.tripleStore == TripleStore.JenaTDB) {
            fusekiServer.stop();
        }
        else if (tripleStore == TripleStore.GraphDB) {
            this.repositoryManager.shutDown();
        }
    }

}
