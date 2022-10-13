package main.java.org.ai.wu.ac.at.rdfstarArchive.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;

import org.ai.wu.ac.at.rdfstarArchive.utils.QueryUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GenerateQueries {

    public static void main(String[] args) throws FileNotFoundException {
        /*
        * Raw query input formats
        * - triple statements
        * - BGPs: e.g. {?s ?p ?o. ?o ?p2 ?o2}
        * 
        * SPARQL query output formats
        * - IC queries = CB queries
        * - TB queries
        * - TB-star_f queries
        * - TB-star_h queries
        */
        final Logger logger = LogManager.getLogger(GenerateQueries.class);

        String inputFilePath = null;
        String outputDirPath = null;
        String input = null;
        String output = null;
        String versions = "0";

        Option inputFileOpt = new Option("r", "inputfile", true, "File with a raw query inside.");
        inputFileOpt.setRequired(true);
        options.addOption(inputFileOpt);

        Option outputDirOpt = new Option("w", "outputdirectory", true, "Directory to which the output SPARQL queries will be written.");
        outputDirOpt.setRequired(true);
        options.addOption(outputDirOpt);

        Option inputFormatOpt = new Option("i", "input", true, "The representation of the raw query. Can be 'ts' or 'bgp'.");
        inputFormatOpt.setRequired(true);
        options.addOption(inputFormatOpt);

        Option outputFormatOpt = new Option("o", "output", true, "The representation of the SPARQL query matching the dataset of a specific archiving policy. Can be IC, CB, TB, TBSF or TBSH.");
        outputFormatOpt.setRequired(true);
        options.addOption(outputFormatOpt);

        Option versionsOpt = new Option("v", "versions", true, "Number of versions. Only needed if o=tb.");
        versionsOpt.setRequired(false);
        options.addOption(versionsOpt);
    
        // Parse input arguments
        CommandLineParser cliParser = new BasicParser();
        CommandLine cmdLine = cliParser.parse(options, args);

        if (cmdLine.hasOption("r")) {
            inputFilePath = cmdLine.getOptionValue("r");
        }

        if (cmdLine.hasOption("w")) {
            outputDirPath = cmdLine.getOptionValue("w");
        }

        if (cmdLine.hasOption("i")) {
            input = cmdLine.getOptionValue("i");
        }

        if (cmdLine.hasOption("o")) {
            output = cmdLine.getOptionValue("o");
        }

        if (cmdLine.hasOption("v")) {
            versions = cmdLine.getOptionValue("v");
        }


        if(input.equalsIgnoreCase("ts")) {
            String query_type="spo";
            String staticVersion = Integer.parseInt(versions);

            File inputFile = new File(inputFilePath);
            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            String line = "";
            PrintWriter pw;

            for (int lines = 0; (line = br.readLine()) != null; lines++) {
                String[] parts = line.split(" ");

                if(output.equalsIgnoreCase("ic") || output.equalsIgnoreCase("cb")) {
                    for(i=0; i<staticVersion; i++) {    
                        String queryString = QueryUtils.createLookupQuery(query_type, parts);
                        String outputFile = new File(outputDirPath + "/" + inputFile.getName() + "_" + lines);
                        pw = new PrintWriter(new FileOutputStream(outputFile), true);
                        pw.append(queryString);
                        pw.close();

                    }
                } else if (output.equalsIgnoreCase("tb")) {
                    String metadataVersions = "<http://www.w3.org/2002/07/owl#versionInfo>";
                    for(i=0; i<staticVersion; i++) {
                        String queryString = QueryUtils.createLookupQueryAnnotatedGraph(query_type, parts, staticVersion, metadataVersions);
                    }
                } else if (output.equalsIgnoreCase("tbsf")) {
                    String initialVersionTS = "2022-01-11T16:51:11.087+02:00";
                    DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSXXXXX");
                    OffsetDateTime version_ts = OffsetDateTime.parse(initialVersionTS, DATE_TIME_FORMATTER);

                    for(i=0; i<staticVersion; i++) {
                        String ueryString = QueryUtils.createLookupQueryRDFStar_f(query_type, parts, version_ts.toString());
                        version_ts = version_ts.plusSeconds(1);
                    }
        
                } else if (output.equalsIgnoreCase("tbsh")) {
                    String initialVersionTS = "2021-12-29T15:44:07.916+02:00";
                    DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSXXXXX");
                    OffsetDateTime version_ts = OffsetDateTime.parse(initialVersionTS, DATE_TIME_FORMATTER);

                    for(i=0; i<staticVersion; i++) {
                        String queryString = QueryUtils.createLookupQueryRDFStar_h(query_type, parts, version_ts.toString());
                        version_ts = version_ts.plusSeconds(1);
                    }
                }
            }
            br.close();

        } else if (input.equalsIgnoreCase("bgp")) {
            if(output.equalsIgnoreCase("ic") || output.equalsIgnoreCase("cb")) {
    
            } else if (output.equalsIgnoreCase("tb")) {
                
            } else if (output.equalsIgnoreCase("tbsf")) {
    
            } else if (output.equalsIgnoreCase("tbsh")) {
    
            }

        }




    }

    
}
