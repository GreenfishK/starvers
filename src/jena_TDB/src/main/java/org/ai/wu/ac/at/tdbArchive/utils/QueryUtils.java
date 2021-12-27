/**
 * 
 */
package org.ai.wu.ac.at.tdbArchive.utils;

import java.util.Iterator;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Resource;

/**
 * @author Javier Fernández
 *
 */
public final class QueryUtils {

	public static final QueryRol getQueryRol(String rol){
		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s") || rol.equalsIgnoreCase("subjects")) {
			return QueryRol.S;
		}
		else if (rol.equalsIgnoreCase("predicate") || rol.equalsIgnoreCase("p") || rol.equalsIgnoreCase("predicates")) {
			return QueryRol.P;
		}
		else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o") || rol.equalsIgnoreCase("objects")) {
			return QueryRol.O;
		}
		else if (rol.equalsIgnoreCase("SP") || rol.equalsIgnoreCase("subjectpredicate")){
			return QueryRol.SP;
		}
		else if (rol.equalsIgnoreCase("SO") || rol.equalsIgnoreCase("subjectobject")){
			return QueryRol.SO;
		}
		else if (rol.equalsIgnoreCase("PO") || rol.equalsIgnoreCase("predicateobject")){
			return QueryRol.PO;
		}
		else if (rol.equalsIgnoreCase("SPO") || rol.equalsIgnoreCase("subjectpredicateobject")){
			return QueryRol.SPO;
		}
		else return QueryRol.ALL;
	}

	public static final String createLookupQuery(String queryType, String term) {
		String[] terms={term};
		return createLookupQuery(queryType,terms);
	}
	public static final String createLookupQuery(String queryType, String[] terms) {
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;
		String queryString="";
		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = "SELECT ?element1 ?element2 WHERE { "+ subject +" ?element1 ?element2 . }";
		}
		else if (qtype==QueryRol.P){
			predicate = terms[0];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?element2 WHERE { ?element1 "+ predicate+" ?element2 . }";
		}
		else if (qtype==QueryRol.O){
			object = terms[0];
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?element2 WHERE { ?element1 ?element2 "+ object+" . }";
		}
		else if (qtype==QueryRol.SP){
			subject = terms[0];
			predicate = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 WHERE { "+ subject +" "+predicate+" ?element1 . }";
		}
		else if (qtype==QueryRol.SO){
			subject = terms[0];
			object = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 WHERE { "+ subject +" ?element1 "+object+" . }";
		}
		else if (qtype==QueryRol.PO){
			predicate = terms[0];
			object = terms[1];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 WHERE { ?element1 "+predicate+ " " +object+" . }";
		}
		else if (qtype==QueryRol.SPO){
			subject = terms[0];
			predicate = terms[1];
			object = terms[2];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT * WHERE { "+subject+" "+predicate+ " " +object+" . }";
		}
		else{ //if (qtype==QueryRol.ALL){
		
			queryString = "SELECT * WHERE { ?element1 ?element2 ?element3 . }";
		}
		

		return queryString;
	}

	public static final String createLookupQueryGraph(String queryType, String term) {
		String[] terms={term};
		return createLookupQueryGraph(queryType,terms);
	}
	
	public static final String createLookupQueryGraph(final String queryType, String[] terms) {
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;
		String queryString="";
			
		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { GRAPH ?graph{ "+ subject +" ?element1 ?element2 . } }";
		}
		else if (qtype==QueryRol.P){
			predicate = terms[0];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { GRAPH ?graph{ ?element1 "+ predicate +" ?element2 . } }";
		}
		else if (qtype==QueryRol.O){
			object = terms[0];
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { GRAPH ?graph{ ?element1 ?element2 "+ object +" . } }";
		}
		else if (qtype==QueryRol.SP){
			subject = terms[0];
			predicate = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?graph WHERE { GRAPH ?graph{ "+ subject +" "+predicate+" ?element1 . }}";
		}
		else if (qtype==QueryRol.SO){
			subject = terms[0];
			object = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?graph WHERE { GRAPH ?graph{ "+ subject +" ?element1 "+object+" . }}";
		}
		else if (qtype==QueryRol.PO){
			predicate = terms[0];
			object = terms[1];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?graph WHERE { GRAPH ?graph{ ?element1 "+predicate+ " " +object+" . }}";
		}
		else if (qtype==QueryRol.SPO){
			subject = terms[0];
			predicate = terms[1];
			object = terms[2];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?graph WHERE { GRAPH ?graph{"+subject+" "+predicate+ " " +object+" . }}";
		}
		else{ //if (qtype==QueryRol.ALL){
		
			queryString = "SELECT * WHERE { GRAPH ?graph{ ?element1 ?element2 ?element3 . }}";
		}
		return queryString;
	}

	public static final String createLookupQueryAnnotatedGraph(String queryType, String term, String metadataVersions) {
		String[] terms={term};
		return createLookupQueryAnnotatedGraph(queryType,terms,metadataVersions);
	}
	
	public static final String createLookupQueryAnnotatedGraph(String queryType, String[] terms, String metadataVersions) {
		String queryString = "";
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;
		
		String graphWHERE= "GRAPH <http://example.org/versions> {?graph " + metadataVersions + " ?version . }\n";
		
		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = "SELECT ?element1 ?element2 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" ?element1 ?element2 ."+"} }";
		}
		else if (qtype==QueryRol.P){
			predicate = terms[0];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?element2 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 "+predicate+" ?element2 ."+"} }";
		}
		else if (qtype==QueryRol.O){
			object = terms[0];
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?element2 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 ?element2 "+object+"."+"} }";
		}
		else if (qtype==QueryRol.SP){
			subject = terms[0];
			predicate = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" "+predicate+" ?element1 ."+"} }";
		}
		else if (qtype==QueryRol.SO){
			subject = terms[0];
			object = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" ?element1 "+object+" ."+"} }";
		}
		else if (qtype==QueryRol.PO){
			predicate = terms[0];
			object = terms[1];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?version WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 "+predicate+" "+object+" ."+"} }";
		}
		else if (qtype==QueryRol.SPO){
			subject = terms[0];
			predicate = terms[1];
			object = terms[2];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?version WHERE { "+graphWHERE + "GRAPH ?graph{ "+subject+" "+predicate+" "+object+" ."+"} }";
		}
		else{ //if (qtype==QueryRol.ALL){
		
			queryString = "SELECT * WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 ?element2 ?element3 . } }";
		}
		

		return queryString;
	}
	/*
	public static final String createLookupQueryAnnotatedGraph2(final String rol, String element, String metadataVersions) {
		String queryString = "";

		queryString = "SELECT ?element1 ?element2 ?version WHERE { " + "GRAPH <http://example.org/versions> {?graph " + metadataVersions
				+ " ?version . }\n"
				// "GRAPH <http://example.org/versions> {?graph "+metadataVersions+" ?x . }\n"
				+ "GRAPH ?graph{";

		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s") || rol.equalsIgnoreCase("subjects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + element + " ?element1 ?element2 .";
		} else if (rol.equalsIgnoreCase("predicate") || rol.equalsIgnoreCase("p") || rol.equalsIgnoreCase("predicates")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 " + element + " ?element2 .";
		} else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o") || rol.equalsIgnoreCase("objects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 ?element2 " + element + " .";
		}

		queryString = queryString + "}" + "}";

		// System.out.println("Query is:"+queryString);
		return queryString;
	}*/

	public static final String createLookupQueryAnnotatedGraph(final String queryType, String term, int staticVersionQuery, String metadataVersions) {
		String[] terms={term};
		return createLookupQueryAnnotatedGraph(queryType,terms,staticVersionQuery,metadataVersions);
	}
	
	public static final String createLookupQueryAnnotatedGraph(final String queryType, String[] terms, int staticVersionQuery, String metadataVersions) {
		String queryString = "";
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;
		
		String graphWHERE= "GRAPH <http://example.org/versions> {?graph " + metadataVersions + " "
				+ staticVersionQuery + " . }\n";
		
		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" ?element1 ?element2 ."+"} }";
		}
		else if (qtype==QueryRol.P){
			predicate = terms[0];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 "+predicate+" ?element2 ."+"} }";
		}
		else if (qtype==QueryRol.O){
			object = terms[0];
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?element2 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 ?element2 "+object+"."+"} }";
		}
		else if (qtype==QueryRol.SP){
			subject = terms[0];
			predicate = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = "SELECT ?element1 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" "+predicate+" ?element1 ."+"} }";
		}
		else if (qtype==QueryRol.SO){
			subject = terms[0];
			object = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ "+subject+" ?element1 "+object+" ."+"} }";
		}
		else if (qtype==QueryRol.PO){
			predicate = terms[0];
			object = terms[1];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT ?element1 ?graph WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 "+predicate+" "+object+" ."+"} }";
		}
		else if (qtype==QueryRol.SPO){
			subject = terms[0];
			predicate = terms[1];
			object = terms[2];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = "SELECT * WHERE { "+graphWHERE + "GRAPH ?graph{ "+subject+" "+predicate+" "+object+" ."+"} }";
		}
		else{ //if (qtype==QueryRol.ALL){
		
			queryString = "SELECT * WHERE { "+ graphWHERE + "GRAPH ?graph{ ?element1 ?element2 ?element3 . } }";
		}
		

		return queryString;
	}


	/*
	public static final String createLookupQueryAnnotatedGraph2(final String rol, String element, int staticVersionQuery, String metadataVersions) {
		String queryString = "";

		queryString = "SELECT ?element1 ?element2 ?graph WHERE { " + "GRAPH <http://example.org/versions> {?graph " + metadataVersions + " "
				+ staticVersionQuery + " . }\n"
				// "GRAPH <http://example.org/versions> {?graph "+metadataVersions+" ?x . }\n"
				+ "GRAPH ?graph{";

		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s") || rol.equalsIgnoreCase("subjects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + element + " ?element1 ?element2 .";
		} else if (rol.equalsIgnoreCase("predicate") || rol.equalsIgnoreCase("p") || rol.equalsIgnoreCase("predicates")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 " + element + " ?element2 .";
		} else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o") || rol.equalsIgnoreCase("objects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 ?element2 " + element + " .";
		}

		queryString = queryString + "}" + "}";

		 //System.out.println("Query is:"+queryString);
		return queryString;
	}*/
	public static final String createLookupQueryAnnotatedGraph(String TP, int staticVersionQuery, String metadataVersions) {
		String queryString = "";

		queryString = "SELECT ?element1 ?element2 ?graph WHERE { " + "GRAPH <http://example.org/versions> {?graph " + metadataVersions + " "
				+ staticVersionQuery + " . }\n"
				// "GRAPH <http://example.org/versions> {?graph "+metadataVersions+" ?x . }\n"
				+ "GRAPH ?graph{";
		queryString = queryString +TP;
		
		queryString = queryString + "}" + "}";

		// System.out.println("Query is:"+queryString);
		return queryString;
	}

    public static final String createLookupQueryRDFStar(final String queryType, String term, String version_ts) {
        String[] terms={term};
		return createLookupQueryRDFStar(queryType,terms,version_ts);
    }

    public static final String createLookupQueryRDFStar(final String queryType, String[] terms, String version_ts) {
        String queryString = "";
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;
		
		//String graphWHERE= "GRAPH <http://example.org/versions> {?graph " + metadataVersions + " "
		//		+ staticVersionQuery + " . }\n";
        String prefixes = "PREFIX vers:<https://github.com/GreenfishK/DataCitation/versioning/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";
		//String version_ts = "2021-12-24T19:25:53.915+02:00"

		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = prefixes
                        + "SELECT ?element1 ?element2 WHERE { "
                        + "<< "+subject+" ?element1 ?element2 >> vers:valid_from ?valid_from. " 
                        + "<< "+subject+" ?element1 ?element2 >> vers:valid_until ?valid_until. " 
                        + "bind(\"" + version_ts + "\"^^xsd:dateTime as ?TimeOfExecution) "
                        + "filter(?valid_from <= ?TimeOfExecution &&  ?TimeOfExecution < ?valid_until)" 
                        + "}";
		}
		else if (qtype==QueryRol.P){
			predicate = terms[0];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = prefixes
            + "SELECT ?element1 ?element2 WHERE { "
            + "<< ?element1 " +predicate+ " ?element2 >> vers:valid_from ?valid_from. " 
            + "<< ?element1 " +predicate+ " ?element2 >> vers:valid_until ?valid_until. " 
            + "bind(\"" + version_ts + "\"^^xsd:dateTime as ?TimeOfExecution) "
            + "filter(?valid_from <= ?TimeOfExecution &&  ?TimeOfExecution < ?valid_until)" 
            + "}";
		}
		else if (qtype==QueryRol.O){
			object = terms[0];
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = prefixes
            + "SELECT ?element1 ?element2 WHERE { "
            + "<< ?element1 ?element2 " +object+ " >> vers:valid_from ?valid_from. " 
            + "<< ?element1 ?element2 " +object+ " >> vers:valid_until ?valid_until. " 
            + "bind(\"" + version_ts + "\"^^xsd:dateTime as ?TimeOfExecution) "
            + "filter(?valid_from <= ?TimeOfExecution &&  ?TimeOfExecution < ?valid_until)" 
            + "}";
		}
		else if (qtype==QueryRol.SP){
			subject = terms[0];
			predicate = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			queryString = prefixes
            + "SELECT ?element1 WHERE { "
            + "<< " + subject + predicate + " ?element1 >> vers:valid_from ?valid_from. " 
            + "<< " + subject + predicate + " ?element1 >> vers:valid_until ?valid_until. " 
            + "bind(\"" + version_ts + "\"^^xsd:dateTime as ?TimeOfExecution) "
            + "filter(?valid_from <= ?TimeOfExecution &&  ?TimeOfExecution < ?valid_until)" 
            + "}";
		}
		else if (qtype==QueryRol.SO){
			subject = terms[0];
			object = terms[1];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = prefixes
            + "SELECT ?element1 WHERE { "
            + "<< "+subject+" ?element1 "+object+" >> vers:valid_from ?valid_from. " 
            + "<< "+subject+" ?element1 "+object+" >> vers:valid_until ?valid_until. " 
            + "bind(\"" + version_ts + "\"^^xsd:dateTime as ?TimeOfExecution) "
            + "filter(?valid_from <= ?TimeOfExecution &&  ?TimeOfExecution < ?valid_until)" 
            + "}";
		}
		else if (qtype==QueryRol.PO){
			predicate = terms[0];
			object = terms[1];
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = prefixes
            + "SELECT ?element1 WHERE { "
            + "<< ?element1 "+predicate+" "+object+" >> vers:valid_from ?valid_from. " 
            + "<< ?element1 "+predicate+" "+object+" >> vers:valid_until ?valid_until. " 
            + "bind(\"" + version_ts + "\"^^xsd:dateTime as ?TimeOfExecution) "
            + "filter(?valid_from <= ?TimeOfExecution &&  ?TimeOfExecution < ?valid_until)" 
            + "}";
		}
		else if (qtype==QueryRol.SPO){
			subject = terms[0];
			predicate = terms[1];
			object = terms[2];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			if (predicate.startsWith("http"))
				predicate = "<" + predicate + ">";
			if (object.startsWith("http"))
				object = "<" + object + ">";
			queryString = prefixes
            + "SELECT * WHERE { "
            + "<< "+subject+" "+predicate+" "+object+" >> vers:valid_from ?valid_from. " 
            + "<< "+subject+" "+predicate+" "+object+" >> vers:valid_until ?valid_until. " 
            + "bind(\"" + version_ts + "\"^^xsd:dateTime as ?TimeOfExecution) "
            + "filter(?valid_from <= ?TimeOfExecution &&  ?TimeOfExecution < ?valid_until)" 
            + "}";
		}
		else{ //if (qtype==QueryRol.ALL){
			queryString = prefixes
            + "SELECT * WHERE { "
            + "<< ?element1 ?element2 ?element3 >> vers:valid_from ?valid_from. " 
            + "<< ?element1 ?element2 ?element3 >> vers:valid_until ?valid_until. " 
            + "bind(\"" + version_ts + "\"^^xsd:dateTime as ?TimeOfExecution) "
            + "filter(?valid_from <= ?TimeOfExecution &&  ?TimeOfExecution < ?valid_until)" 
            + "}";
		}
		

		return queryString;
    }
	
	public static final String createTPLookupQuery(final String rol, String element) {
		String queryString = "";
		if (rol.equalsIgnoreCase("subject") || rol.equalsIgnoreCase("s") || rol.equalsIgnoreCase("subjects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + element + " ?element1 ?element2 .";
		} else if (rol.equalsIgnoreCase("predicate") || rol.equalsIgnoreCase("p") || rol.equalsIgnoreCase("predicates")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 " + element + " ?element2 .";
		} else if (rol.equalsIgnoreCase("object") || rol.equalsIgnoreCase("o") || rol.equalsIgnoreCase("objects")) {
			if (element.startsWith("http"))
				element = "<" + element + ">";
			queryString = queryString + "?element1 ?element2 " + element + " .";
		}
		

		return queryString;
	}

	public static final String getGraphs() {
		String queryString = "SELECT distinct ?graph WHERE { " + "GRAPH ?graph{";

		queryString = queryString + " ?element1 ?element2 ?element3 .";

		queryString = queryString + "}" + "}";

		return queryString;
	}

	public static final String getNumGraphVersions(String metadataVersions) {
		String queryString = "SELECT (count(distinct ?element1) as ?numVersions)  WHERE { " + "GRAPH <http://example.org/versions> {?graph "
				+ metadataVersions + " ?element1 . }}";

		return queryString;
	}

	/**
	 * @param soln
	 * @return
	 * @return
	 */
	public static final String serializeSolution(QuerySolution soln) {
		Iterator<String> vars = soln.varNames();
		String rowResult = "";
		while (vars.hasNext()) {
			String var = vars.next();
			if (soln.get(var).isResource()) {
				Resource rs = (Resource) soln.get(var);
				rowResult += "<" + rs.getURI() + "> ";
			} else {
				//rowResult += soln.getLiteral(var).asLiteral().getValue() + " ";
				rowResult += soln.getLiteral(var).getString();
			}
		}
		return rowResult.trim();
	}

	/**
	 * @param soln
	 * @return
	 * @return
	 */
	public static final String serializeSolutionFilterOutGraphs(QuerySolution soln) {
		Iterator<String> vars = soln.varNames();
		String rowResult = "";
		while (vars.hasNext()) {
			String var = vars.next();
			if (!var.equalsIgnoreCase("graph")&& !var.equalsIgnoreCase("version")) {
				if (soln.get(var).isResource()) {
					Resource rs = (Resource) soln.get(var);
					rowResult += "<" + rs.getURI() + "> ";
				} else {
					//rowResult += soln.getLiteral(var).asLiteral().getValue() + " ";
					rowResult += soln.getLiteral(var).getString();
				}
			}
		}
		return rowResult.trim();
	}

    public static final int getLimit(String[] terms) {
        if (terms.length > 4) {
            return Integer.parseInt(terms[3]) + Integer.parseInt(terms[4]);
        }
        return Integer.MAX_VALUE;
    }

}
