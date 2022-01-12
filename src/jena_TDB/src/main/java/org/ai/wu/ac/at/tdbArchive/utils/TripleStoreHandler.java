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

    public void load(String directory, TripleStore tripleStore) {
        if (tripleStore == TripleStore.JenaTDB) {
            this.tripleStore = TripleStore.JenaTDB;

            // Initialize Jena
            ARQ.init();
            FileManager fm = FileManager.get();
            fm.addLocatorClassLoader(JenaTDBArchive_query.class.getClassLoader());

            // Create a TDB persistent dataset in tmp/TDB/currentTimestamp and load the .nq file into it.
            this.tripleStoreLoc = "target/TDB";
            DatasetGraphTDB dsg = DatasetBuilderStd.create(Location.create(this.tripleStoreLoc));
            logger.info(String.format("If you are using docker the TDB dataset will be located " +
                    "in /var/lib/docker/overlay2/<buildID>/diff/%s", this.tripleStoreLoc));
            InputStream in = fm.open(directory);
            long startTime = System.currentTimeMillis();
            TDBLoader.load(dsg, in, Lang.NQ,false, true);
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
            dataset.end();

        }
        else if (tripleStore == TripleStore.GraphDB) {

        }

    }

    public void shutdown() {
        if (this.tripleStore == TripleStore.JenaTDB) {
            fusekiServer.stop();
        }
        else if (tripleStore == TripleStore.GraphDB) {

        }
    }
}
