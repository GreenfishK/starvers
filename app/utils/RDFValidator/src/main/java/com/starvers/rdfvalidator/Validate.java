package com.starvers.rdfvalidator;

import org.eclipse.rdf4j.rio.RDFParserFactory;
import org.eclipse.rdf4j.rio.RDFParserRegistry;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.nquads.NQuadsParserFactory;
import org.eclipse.rdf4j.rio.ntriples.NTriplesParserFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.FileWriter;

import java.util.Scanner;


public class Validate {

    public static void main(String[] args) throws Exception {
        String[] splitFileName = args[0].split("\\.(?=[^\\.]+$)");
        String cleanDatasetFile = args[1];
        String extension = splitFileName[1];

        FileInputStream inputStream = null;
        Scanner sc = null;

        // Setting format
        RDFFormat format = null;
        RDFParserRegistry parserRegistry = RDFParserRegistry.getInstance();
        if (extension.equals("nt")) {
            format = RDFFormat.NTRIPLES;
            parserRegistry.add(new NTriplesParserFactory());
        } else if (extension.equals("nq")) {
            format = RDFFormat.NQUADS;
            parserRegistry.add(new NQuadsParserFactory());
        } else {
            System.out.println("Extension must be .nt or .nq");
            return;
        }

        // GraphDB parser initialization
        RDFParserFactory factory = parserRegistry.get(format).get();
        RDFParser rdfParser = factory.getParser();

        FileWriter writer = null;
        FileWriter logWriter = null;

        try {
            inputStream = new FileInputStream(new File(args[0]));
            sc = new Scanner(inputStream, "UTF-8");

            writer = new FileWriter(cleanDatasetFile);
            logWriter = new FileWriter("/data/logs/RDF-validation.log");

            int i = 0;

            while (sc.hasNextLine()) {
                String nextLine = sc.nextLine();
                InputStream triple = null;

                try {
                    triple = new ByteArrayInputStream(nextLine.getBytes());
                    rdfParser.parse(triple, null);
                } catch (IOException e) {
                    logWriter.write("IOException on line " + (i + 1) + ": " + e.getMessage() + System.lineSeparator());
                    nextLine = "# " + nextLine;
                } catch (RDFParseException e) {
                    logWriter.write("RDFParseException on line " + (i + 1) + ": " + e.getMessage() + System.lineSeparator());
                    logWriter.write("Invalid triple: " + nextLine + System.lineSeparator());
                    nextLine = "# " + nextLine;
                } catch (RDFHandlerException e) {
                    logWriter.write("RDFHandlerException on line " + (i + 1) + ": " + e.getMessage() + System.lineSeparator());
                    logWriter.write("Invalid triple: " + nextLine + System.lineSeparator());
                    nextLine = "# " + nextLine;
                } finally {
                    if (triple != null) {
                        triple.close();
                    }
                }

                writer.write(nextLine + System.lineSeparator());
                i++;
            }

            if (sc.ioException() != null) {
                throw sc.ioException();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (inputStream != null) inputStream.close();
            if (sc != null) sc.close();
            if (writer != null) writer.close();
            if (logWriter != null) logWriter.close();
        }
    }
}