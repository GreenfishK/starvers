import org.apache.jena.graph.Graph;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.Lang;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ARQ;
import org.apache.jena.riot.RiotException;

import java.io.FileInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.FileWriter;

import java.nio.file.Files;
import java.nio.file.Path;

import java.util.ArrayList;


public class Validate {

    public static void main(String [] args) {
        ARQ.init();
        
        String content = "";
        String [] splitFileName = args[0].split("\\.");
        String extension = splitFileName[splitFileName.length - 1];

        try {
            content = Files.readString(Path.of(args[0]));
        } catch(IOException e) {
            e.printStackTrace();
        }
        String [] triples = content.split(System.lineSeparator());
        System.out.println("Number of lines in " + args[0] + ": " + triples.length);

        final Graph g = ModelFactory.createDefaultModel().getGraph();
        final StreamRDF dest = StreamRDFLib.graph(g); 
        ArrayList<Integer> invalidLines = new ArrayList<Integer>();
        for (int i=0;i<triples.length;i++) {
            try {
                    Lang l = null;
                    if (extension.equals("nt")) {
                        l = Lang.NT;
                    } else if (extension.equals("nq")) {
                        l = Lang.NQ;
                    } else {
                        System.out.println("Extension must be .nt or .nq");
                    }
                    RDFParser.fromString(triples[i]).lang(l).parse(dest);
                
            } catch(RiotException e) {
                invalidLines.add(i+1);
            }
        }
        System.out.println("Number of invalid lines: " + invalidLines.size());
        try {
            FileWriter writer = new FileWriter(args[1]); 
            for(Integer invalidLine: invalidLines) {
                writer.write(Integer.toString(invalidLine) + System.lineSeparator());
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
