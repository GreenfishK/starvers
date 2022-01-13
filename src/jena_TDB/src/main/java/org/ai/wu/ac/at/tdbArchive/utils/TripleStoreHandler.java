package org.ai.wu.ac.at.tdbArchive.utils;

import org.ai.wu.ac.at.tdbArchive.api.JenaTDBArchive.TripleStore;
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

import java.io.*;

import com.ontotext.trree.config.OWLIMSailSchema;

import org.eclipse.rdf4j.common.io.FileUtil;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Triple;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.Query;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.base.RepositoryConnectionWrapper;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.manager.LocalRepositoryManager;
import org.eclipse.rdf4j.repository.sail.config.SailRepositorySchema;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.openrdf.model.util.GraphUtilException;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

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

    public void load(String directory, String format, TripleStore tripleStore) {

        if (tripleStore == TripleStore.JenaTDB) {
            this.tripleStoreLoc = "target/TDB";
            Lang rdf_format = null;
            switch (format) {
                case "nq": rdf_format = Lang.NQ; break;
                case "ttl": rdf_format = Lang.TTL; break;
            }
            logger.info("Initializing Jena Fuseki Server");
            this.tripleStore = TripleStore.JenaTDB;

            // Initialize Jena
            ARQ.init();

            // Create a TDB persistent dataset in tmp/TDB/currentTimestamp and load the .nq file into it.
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
            this.endpoint = String.format("http://localhost:%d/evalJenaTDB/sparql", fusekiServer.getHttpPort());
            logger.info("Jena Fuseki Server endpoint: " + this.endpoint);
            dataset.end();

        }
        else if (tripleStore == TripleStore.GraphDB) {
            try {
                this.tripleStoreLoc = "target/GraphDB";
                File baseDir = new File("target","GraphDB");
                baseDir.mkdirs();
                repositoryManager = new LocalRepositoryManager(baseDir);
                repositoryManager.init();

                if (repositoryManager.hasRepositoryConfig("evalGraphDB")) {
                    throw new RuntimeException("Repository evalGraphDB already exists.");
                }

                TreeModel repo_config_graph = new TreeModel();

                InputStream config = TripleStoreHandler.class.getResourceAsStream("/repo-defaults.ttl");
                RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
                rdfParser.setRDFHandler(new StatementCollector(repo_config_graph));
                rdfParser.parse(config, RepositoryConfigSchema.NAMESPACE);
                assert config != null;
                config.close();

                Resource repositoryNode = Models.subject(repo_config_graph.filter(null, RDF.TYPE, RepositoryConfigSchema.REPOSITORY)).orElse(null);
                repo_config_graph.add(repositoryNode, RepositoryConfigSchema.REPOSITORYID,
                        SimpleValueFactory.getInstance().createLiteral("evalGraphDB"));

                RepositoryConfig repositoryConfig = RepositoryConfig.create(repo_config_graph, repositoryNode);
                repositoryManager.addRepositoryConfig(repositoryConfig);

                this.endpoint = String.format("http://localhost:%d/repositories/evalGraphDB", 7200);


            } catch (RDFHandlerException | RepositoryConfigException | RDFParseException | IOException | RepositoryException e) {
                logger.error("The GraphDB repository will not be created.");
                logger.error(e.getMessage());
                System.out.println(e.getMessage());
            }

        }

    }

    public String connectToRepo() {
        if (this.tripleStore == TripleStore.JenaTDB) {
            //The connection to Jena is established via RDFConnection by the caller.
            RDFConnection conn = RDFConnection.connect(this.endpoint);
            logger.info("You are now connected to JenaTDB and endpoint: " + this.endpoint);
        }
        else if (tripleStore == TripleStore.GraphDB) {
            repositoryManager.getRepository("evalGraphDB").getConnection().commit();
            logger.info("You are now connected to GraphDB and endpoint: " + this.endpoint);
        }
        return this.endpoint;
    }

    public void disconnectRepo() {
        if (this.tripleStore == TripleStore.JenaTDB) {
            //The disconnection to Jena is established via RDFConnection by the caller.
            logger.info("You are now disconnected from JenaTDB and endpoint: " + this.endpoint);
        }
        else if (tripleStore == TripleStore.GraphDB) {
            repositoryManager.getRepository("evalGraphDB").getConnection().close();
            logger.info("You are now disconnected from GraphDB and endpoint: " + this.endpoint);
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
