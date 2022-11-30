import org.apache.jena.graph.Graph;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.Lang;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ARQ;

import java.io.FileInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.nio.file.Files;
import java.nio.file.Path;


public class Validate {

    public static void main(String [] args) {
        ARQ.init();
        
        try {
            String content = Files.readString(Path.of(args[0]));
        } catch(IOException e) {
            e.printStackTrace();
        }

        final Graph g = ModelFactory.createDefaultModel().getGraph();
        final StreamRDF dest = StreamRDFLib.graph(g); 
        try {
            RDFParser.source(content).parse(dest);
        } catch(Exception e) {
            System.out.println(e.getMessage());
            //BufferedReader bufReader = new BufferedReader(new StringReader(textContent));

        }

    }
    
}
