package com.starvers.rdfvalidator;

import org.apache.jena.graph.Graph;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.riot.Lang;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.query.ARQ;
import org.apache.jena.riot.RiotException;

import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFHandlerException;


import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileWriter;

import java.util.Scanner;


public class Validate {

    public static void main(String [] args) throws Exception {
        ARQ.init();
        
        String [] splitFileName = args[0].split("\\.(?=[^\\.]+$)");
        String cleanDatasetFile = args[1];

        String extension = splitFileName[1];
        final Graph g = ModelFactory.createDefaultModel().getGraph();
        final StreamRDF dest = StreamRDFLib.graph(g); 
        FileInputStream inputStream = null;
        Scanner sc = null;

        Lang l = null;
        RDFFormat format = null;
        if (extension.equals("nt")) {
            l = Lang.NT;
            format = RDFFormat.NTRIPLES;
        } else if (extension.equals("nq")) {
            l = Lang.NQ;
            format = RDFFormat.NQUADS;
        } else {
            System.out.println("Extension must be .nt or .nq");
        }

        try {
            inputStream = new FileInputStream(new File(args[0]));
            sc = new Scanner(inputStream, "UTF-8");
            FileWriter writer = new FileWriter(cleanDatasetFile);
            int i = 0;
            
            while (sc.hasNextLine()) {
                String nextLine = sc.nextLine();
                // Jena parser
                /*try {
                    org.apache.jena.riot.RDFParser.fromString(nextLine).lang(l).parse(dest);
                } catch(RiotException e) {
                    System.out.println("jena:RiotException: " + e.getMessage());
                    System.out.println("jena:Invalid line: " + Integer.toString(i+1));
                    nextLine = "# " + nextLine;
                } catch(Exception e) {
                    System.out.println("jena:Exception at line: " + Integer.toString(i+1) + ":" + e.getMessage());
                    //nextLine = "# " + nextLine;
                }  catch(Error e) {
                    System.out.println("jena:Error at line: " + Integer.toString(i+1) + ":" + e.getMessage());
                    System.out.println("jena:Probably due to blank nodes. Line will not be counted as invalid");
                }*/
                //rdf4j parser
                InputStream triple = null;
                try {
                    triple = new ByteArrayInputStream(nextLine.getBytes());
                    Rio.parse(triple, format);
                }
                catch (IOException e) {
                    System.out.println(e.getMessage());
                }
                catch (RDFParseException e) {
                        System.out.println("rdf4j:RDFParseException: " + e.getMessage());
                        System.out.println("rdf4j:Invalid line: " + Integer.toString(i+1));
                        nextLine = "# " + nextLine;
                }
                catch (RDFHandlerException e) {
                        System.out.println("rdf4j:RDFHandlerException: " + e.getMessage());
                        System.out.println("rdf4j:Invalid line: " + Integer.toString(i+1));
                        nextLine = "# " + nextLine;
                }
                finally {
                    triple.close();
                }                               
                i++;
                try {
                    writer.write(nextLine + System.lineSeparator());              
                } catch (IOException e) {
                    e.printStackTrace();
                }
                if (i % 1000000 == 0) {
                    System.gc();
                }
            }
            writer.close();

            
            
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (sc != null) {
                sc.close();
            }
        }
    }
}
