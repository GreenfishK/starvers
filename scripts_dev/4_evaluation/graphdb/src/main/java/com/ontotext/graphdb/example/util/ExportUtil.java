package com.ontotext.graphdb.example.util;

import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Utility methods for exporting data from a GraphDB repository.
 */
public class ExportUtil {
    public enum ExportType {
        EXPLICIT, IMPLICIT, ALL;
    }

    /**
     * Export the contents of the repository (explicit, implicit or all statements) to the given filename in
     * the given RDF format,
     *
     * @param repositoryConnection a connection to a reposityr
     * @param filename             file to export to (the format will be inferred from the extension)
     * @param type                 what to export (implicit, explicit or all statements)
     * @throws RepositoryException
     * @throws UnsupportedRDFormatException
     * @throws IOException
     * @throws RDFHandlerException
     */
    public static void export(RepositoryConnection repositoryConnection, String filename, ExportType type)
            throws RepositoryException, UnsupportedRDFormatException, IOException, RDFHandlerException {
        RDFFormat exportFormat = Rio.getWriterFormatForFileName(filename).orElse(null);
        if (exportFormat == null) {
            throw new RuntimeException("Unknown export format requested.");
        }

        System.out.println("Exporting " + type + " statements to " + filename + " (" + exportFormat.getName() + ")");

        Writer writer = new BufferedWriter(new FileWriter(filename), 256 * 1024);
        RDFWriter rdfWriter = Rio.createWriter(exportFormat, writer);

        // This approach to making a backup of a repository by using RepositoryConnection.exportStatements()
        // will work even for very large remote repositories, because the results are streamed to the client
        // and passed directly to the RDFHandler.
        // However, it is not possible to give any indication of progress using this method.
        try {
            switch (type) {
                case EXPLICIT:
                    repositoryConnection.exportStatements(null, null, null, false, rdfWriter);
                    break;
                case ALL:
                    repositoryConnection.exportStatements(null, null, null, true, rdfWriter);
                    break;
                case IMPLICIT:
                    repositoryConnection.exportStatements(null, null, null, true, rdfWriter,
                    		repositoryConnection.getValueFactory().createIRI("http://www.ontotext.com/implicit"));
                    break;
            }
        } finally {
            writer.close();
        }
    }

}
