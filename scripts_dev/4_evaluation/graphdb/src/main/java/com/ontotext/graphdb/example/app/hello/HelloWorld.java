package com.ontotext.graphdb.example.app.hello;

import com.ontotext.graphdb.example.util.EmbeddedGraphDB;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

/**
 * Hello World app for GraphDB
 */
public class HelloWorld {

    public static void main(String[] args) throws Exception {
        HTTPRepository repository = new HTTPRepository(args[0]);
        RepositoryConnection connection = repository.getConnection();
        System.out.println("Connection established");

        // Preparing a SELECT query for later evaluation
        TupleQuery tupleQuery = connection.prepareTupleQuery(QueryLanguage.SPARQL,
        "SELECT ?s ?p ?o WHERE {" +
                "?s ?p ?o ." +
                "} limit 10");

        TupleQueryResult tupleQueryResult = tupleQuery.evaluate();
        while (tupleQueryResult.hasNext()) {
            BindingSet bindingSet = tupleQueryResult.next();
            for (Binding binding : bindingSet) {
                // Each Binding contains the variable name and the value for this result row
                String name = binding.getName();
                Value value = binding.getValue();
                System.out.println(name + " = " + value);
            }
        }

        tupleQueryResult.close();

        if(args.length > 1 && args[1].equals("close")) {
            connection.close();
            System.out.println("Connection closed");

        }
    }
}
