package com.ontotext.graphdb.example.util.chunkloader;

import com.ontotext.trree.GraphDBParserConfig;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;

import java.io.*;
import java.util.zip.GZIPInputStream;

/**
 * Helper class for loading large datasets with chunking.
 * Chunk loading works by loading portions (chunks)
 * of the dataset and committing the transaction after each chunk.
 */
public class ChunkLoader {
    private boolean preserveBnodeIds;
    private boolean parallelInsertion;
    private long chunkSize;

    public ChunkLoader(long chunkSize, boolean preserveBnodeIds, boolean parallelInsertion) {
        this.chunkSize = chunkSize;
        this.preserveBnodeIds = preserveBnodeIds;
        this.parallelInsertion = parallelInsertion;
    }

    public long loadFile(RepositoryConnection repositoryConnection, File file, Resource context)
            throws IOException, RDFParseException, RDFHandlerException, RepositoryException {
        RDFFormat format = Rio.getParserFormatForFileName(file.getName()).orElse(null);

        if (format == null) {
            throw new IOException("Unknown format for file.");
        }

        InputStream reader = null;

        try {
            if (file.getName().endsWith("gz")) {
                reader = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file), 256 * 1024));
            } else {
                reader = new BufferedInputStream(new FileInputStream(file), 256 * 1024);
            }

            long start = System.currentTimeMillis();

            ParserConfig config = GraphDBParserConfig.newInstance();
            config.set(BasicParserSettings.PRESERVE_BNODE_IDS, preserveBnodeIds);

            RDFParser parser = Rio.createParser(format);
            parser.setParserConfig(config);

            // add our own custom RDFHandler to the parser. This handler takes care of adding
            // triples to our repository and doing intermittent commits
            ChunkCommitter handler = new ChunkCommitter(repositoryConnection, context, chunkSize);
            parser.setRDFHandler(handler);

            repositoryConnection.begin();

            if (parallelInsertion) {
                IRI up = SimpleValueFactory.getInstance().createIRI("http://www.ontotext.com/useParallelInsertion");
                repositoryConnection.add(up, up, up);
            }

            Resource importContext = context == null ? SimpleValueFactory.getInstance().createIRI(file.toURI().toString()) : context;
            parser.parse(reader, importContext.toString());

            repositoryConnection.commit();

            long statementsLoaded = handler.getStatementCount();
            long time = System.currentTimeMillis() - start;

            System.out.println("Loaded " + statementsLoaded + " statements in " + time + " ms; avg speed = " + (statementsLoaded * 1000 / time) + " st/s");

            return statementsLoaded;
        } catch (RepositoryException | RDFParseException | RDFHandlerException e) {
            try {
                repositoryConnection.rollback();
            } catch (RepositoryException ex) {
                // ignored
            }
            throw e;
        } finally {
            if (reader != null)
                reader.close();
        }
    }

}
