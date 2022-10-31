package com.ontotext.graphdb.example.util;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;

/**
 * Utility methods for RepositoryConnection.
 */
public class ConnectionUtil {
    /**
     * Retrieves the number of explicit statements in a repository.
     * The default implementation delegates to connection.size().
     *
     * @param repositoryConnection a connection to a repository
     * @return the number of explicit statements in the repository
     * @throws Exception
     */
    public static long numberOfExplicitStatements(RepositoryConnection repositoryConnection) throws Exception {
        // Getting the number of explicit statements through the Sesame API
        return repositoryConnection.size();

        /*
        // Another approach is to get an iterator to the explicit statements
        // (by setting the includeInferred parameter to false) and then count them.
        long explicitStatements = 0;

        RepositoryResult<Statement> statements = repositoryConnection.getStatements(null, null, null, false);
        explicitStatements = 0;

        while (statements.hasNext()) {
            statements.next();
            explicitStatements++;
        }
        statements.close();

        return explicitStatements;
        */
    }

    /**
     * Retrieves the number of implicit statements in a repository.
     * <p/>
     * No method for this is available through the Sesame API, so GraphDB uses
     * a special context that is interpreted as an instruction to retrieve only
     * the implicit statements, i.e. not explicitly asserted in the repository.
     *
     * @param repositoryConnection a connection to a repository
     * @return the number of implicit statements in the repository
     * @throws Exception
     */
    public static long numberOfImplicitStatements(RepositoryConnection repositoryConnection) throws Exception {
        // Retrieve all inferred statements
        RepositoryResult<Statement> statements = repositoryConnection.getStatements(null, null, null, true,
                repositoryConnection.getValueFactory().createIRI("http://www.ontotext.com/implicit"));
        long implicitStatements = 0;

        while (statements.hasNext()) {
            statements.next();
            implicitStatements++;
        }
        statements.close();

        return implicitStatements;
    }

}
