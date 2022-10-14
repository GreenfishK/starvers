package filip.test;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.query.*;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.riot.Lang;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb.TDBLoader;
import org.apache.jena.tdb.base.file.Location;
import org.apache.jena.tdb.setup.DatasetBuilderStd;
import org.apache.jena.tdb.store.DatasetGraphTDB;
import org.apache.jena.util.FileManager;
import java.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Hello world!
 *
 */
public class App 
{
    //private static final Logger logger = LogManager.getLogger(App.class);

    public static void main( String[] args )
    {

        // Initialize Jena
        //ARQ.init();

        // Create a TDB persistent dataset in tmp/TDB/currentTimestamp and load the .nq file into it.
        DatasetGraphTDB dsg = DatasetBuilderStd.create(Location.create("target/TDB"));

        FileManager fm = FileManager.get();
        fm.addLocatorClassLoader(App.class.getClassLoader());
        InputStream in = fm.open("~/.BEAR/rawdata/bearb/hour/alldata.CB_computed.nt/data-added_1-2.nt");
        TDBLoader.load(dsg, in, Lang.NT, false, true);

        // Create a dataset object from the persistent TDB dataset
        Dataset dataset = TDBFactory.createDataset("target/TDB");
        
        // Create a fuseki server, load the dataset object into the repository
        System.out.println("Initializing Jena Fuseki Server");
        FusekiServer fusekiServer;
        fusekiServer = FusekiServer.create()
                                   .add("/evalJenaTDB", dataset)
                                   .build();
        fusekiServer.start();

        String endpoint = String.format("http://localhost:%d/evalJenaTDB/sparql", fusekiServer.getHttpPort());
        System.out.println(endpoint);
        System.out.println("Jena Fuseki Server endpoint: " + endpoint);
        dataset.end();
    }
}
