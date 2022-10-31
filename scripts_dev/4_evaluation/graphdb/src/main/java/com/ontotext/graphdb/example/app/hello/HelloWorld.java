package com.ontotext.graphdb.example.app.hello;

import com.ontotext.graphdb.example.util.EmbeddedGraphDB;
import org.eclipse.rdf4j.repository.RepositoryConnection;

/**
 * Hello World app for GraphDB
 */
public class HelloWorld {

    public static void main(String[] args) throws Exception {
        HTTPRepository repository = new HTTPRepository(args[0]);
        RepositoryConnection connection = repository.getConnection();
        System.out.println("Connection established");

        if(args.length > 1 && args[1].equals("close")) {
            connection.close();
            System.out.println("Connection closed");

        }
    }
}
