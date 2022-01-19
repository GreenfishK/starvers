/**
 * 
 */
package org.ai.wu.ac.at.tdbArchive.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.ai.wu.ac.at.tdbArchive.api.TripleStore;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_TB_star;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;

/**
 * @author Javier Fernández
 *
 */
public final class QueryUtils {
	private static final Logger logger = LogManager.getLogger(QueryUtils.class);

	public static void logQueryStatistics(TripleStore tripleStore, String filePath, TreeMap<Integer, DescriptiveStatistics> vStats)
			throws FileNotFoundException {
		File f = new File(filePath);
		PrintWriter pw;
		if ( f.exists() && !f.isDirectory() ) {
			pw = new PrintWriter(new FileOutputStream(filePath, true));
		}
		else {
			pw = new PrintWriter(filePath);
			pw.append("tripleStore,ver,min,mean,max,stddev,count,sum\n");
		}

		for (Map.Entry<Integer, DescriptiveStatistics> ent : vStats.entrySet()) {
			pw.append(tripleStore + "," + ent.getKey() + "," + ent.getValue().getMin() + ","
					+ ent.getValue().getMean() + "," + ent.getValue().getMax() + ","
					+ ent.getValue().getStandardDeviation() + "," + ent.getValue().getN()
					+ ","+ent.getValue().getSum() + "\n");
		}
		pw.close();
	}

	public static QueryRol getQueryRol(String rol){
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

	public static String createLookupQuery(String queryType, String term) {
		String[] terms={term};
		return createLookupQuery(queryType,terms);
	}
	public static String createLookupQuery(String queryType, String[] terms) {
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

	public static String createLookupQueryGraph(String queryType, String term) {
		String[] terms={term};
		return createLookupQueryGraph(queryType,terms);
	}
	
	public static String createLookupQueryGraph(final String queryType, String[] terms) {
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

	public static String createLookupQueryAnnotatedGraph(String queryType, String term, String metadataVersions) {
		String[] terms={term};
		return createLookupQueryAnnotatedGraph(queryType,terms,metadataVersions);
	}
	
	public static String createLookupQueryAnnotatedGraph(String queryType, String[] terms, String metadataVersions) {
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

	public static String createLookupQueryAnnotatedGraph(final String queryType, String term, int staticVersionQuery, String metadataVersions) {
		String[] terms={term};
		return createLookupQueryAnnotatedGraph(queryType,terms,staticVersionQuery,metadataVersions);
	}
	
	public static String createLookupQueryAnnotatedGraph(final String queryType, String[] terms, int staticVersionQuery, String metadataVersions) {
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
	public static String createLookupQueryAnnotatedGraph(String TP, int staticVersionQuery, String metadataVersions) {
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

    public static String createLookupQueryRDFStar_f(final String queryType, String term, String version_ts) {
        String[] terms={term};
		return createLookupQueryRDFStar_f(queryType,terms,version_ts);
    }

    public static String createLookupQueryRDFStar_f(final String queryType, String[] terms, String version_ts) {
        String queryString = "";
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;

        String prefixes = "PREFIX vers:<https://github.com/GreenfishK/DataCitation/versioning/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

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

	public static String createLookupQueryRDFStar_h(final String queryType, String term, String version_ts) {
		String[] terms={term};
		return createLookupQueryRDFStar_h(queryType,terms,version_ts);
	}

	public static String createLookupQueryRDFStar_h(final String queryType, String[] terms, String version_ts) {
		String queryString = "";
		QueryRol qtype = getQueryRol(queryType);
		String subject, predicate, object;

		String prefixes = "PREFIX vers:<https://github.com/GreenfishK/DataCitation/versioning/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

		if (qtype==QueryRol.S){
			subject = terms[0];
			if (subject.startsWith("http"))
				subject = "<" + subject + ">";
			queryString = prefixes
					+ "SELECT ?element1 ?element2 WHERE { "
					+ "<<<< "+subject+" ?element1 ?element2 >> vers:valid_from ?valid_from >> vers:valid_until ?valid_until. "
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
					+ "<<<< ?element1 " +predicate+ " ?element2 >> vers:valid_from ?valid_from >> vers:valid_until ?valid_until."
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
					+ "<<<< ?element1 ?element2 " +object+ " >> vers:valid_from ?valid_from >> vers:valid_until ?valid_until. "
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
					+ "<<<< " + subject + predicate + " ?element1 >> vers:valid_from ?valid_from >> vers:valid_until ?valid_until. "
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
					+ "<<<< "+subject+" ?element1 "+object+" >> vers:valid_from ?valid_from >> vers:valid_until ?valid_until. "
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
					+ "<<<< ?element1 "+predicate+" "+object+" >> vers:valid_from ?valid_from >> vers:valid_until ?valid_until. "
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
					+ "<<<< "+subject+" "+predicate+" "+object+" >> vers:valid_from ?valid_from >> vers:valid_until ?valid_until. "
					+ "bind(\"" + version_ts + "\"^^xsd:dateTime as ?TimeOfExecution) "
					+ "filter(?valid_from <= ?TimeOfExecution &&  ?TimeOfExecution < ?valid_until)"
					+ "}";
		}
		else{ //if (qtype==QueryRol.ALL){
			queryString = prefixes
					+ "SELECT * WHERE { "
					+ "<<<< ?element1 ?element2 ?element3 >> vers:valid_from ?valid_from >> vers:valid_until ?valid_until. "
					+ "bind(\"" + version_ts + "\"^^xsd:dateTime as ?TimeOfExecution) "
					+ "filter(?valid_from <= ?TimeOfExecution &&  ?TimeOfExecution < ?valid_until)"
					+ "}";
		}


		return queryString;
	}

	public static String createTPLookupQuery(final String rol, String element) {
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

	public static String getGraphs() {
		String queryString = "SELECT distinct ?graph WHERE { " + "GRAPH ?graph{";

		queryString = queryString + " ?element1 ?element2 ?element3 .";

		queryString = queryString + "}" + "}";

		return queryString;
	}

	public static String getNumGraphVersions(String metadataVersions) {
		String queryString = "SELECT (count(distinct ?element1) as ?numVersions)  WHERE { " + "GRAPH <http://example.org/versions> {?graph "
				+ metadataVersions + " ?element1 . }}";

		return queryString;
	}

	/**
	 * @param soln
	 * @return
	 * @return
	 */
	public static String serializeSolution(QuerySolution soln) {
		Iterator<String> vars = soln.varNames();
		String rowResult = "";
		while (vars.hasNext()) {
			String var = vars.next();
			if (soln.get(var).isResource()) {
				Resource rs = (Resource) soln.get(var);
				rowResult += "<" + rs.getURI() + "> ";
			} else {
				rowResult += soln.getLiteral(var).getString();
			}
		}
		return rowResult.trim();
	}

	public static String serializeSolution(BindingSet bindingSet) {
		StringBuilder resultRow = new StringBuilder();
		for (Binding binding : bindingSet) {
			resultRow.append(binding.getValue().stringValue()).append(" ");
		}
		return resultRow + ".";
	}

	/**
	 * @param soln
	 * @return
	 * @return
	 */
	public static String serializeSolutionFilterOutGraphs(QuerySolution soln) {
		Iterator<String> vars = soln.varNames();
		String rowResult = "";
		while (vars.hasNext()) {
			String var = vars.next();
			if (!var.equalsIgnoreCase("graph")&& !var.equalsIgnoreCase("version")) {
				if (soln.get(var).isResource()) {
					Resource rs = (Resource) soln.get(var);
					rowResult += "<" + rs.getURI() + "> ";
				} else {
					rowResult += soln.getLiteral(var).getString();
				}
			}
		}
		return rowResult.trim();
	}

	public static String serializeSolutionFilterOutGraphs(BindingSet bindingSet) {
		StringBuilder resultRow = new StringBuilder();
		for (Binding binding : bindingSet) {
			if (!binding.getName().equalsIgnoreCase("graph")
					&& !binding.getName().equalsIgnoreCase("version")) {
				resultRow.append(binding.getValue().stringValue()).append(" ");
			}
		}
		return resultRow + ".";
	}

    public static int getLimit(String[] terms) {
        if (terms.length > 4) {
            return Integer.parseInt(terms[3]) + Integer.parseInt(terms[4]);
        }
        return Integer.MAX_VALUE;
    }

	public static String getVersionInfos_f() {
		String queryString = "Select (count(distinct ?ts) -1 as ?cnt_versions)\n" +
				" (min(?ts) as ?initial_version_ts) where {\n" +
				"    {\n" +
				"        select \n" +
				"        distinct (?valid_from as ?ts) where {\n" +
				"            <<?s ?p ?o>> <https://github.com/GreenfishK/DataCitation/versioning/valid_from> ?valid_from.\n" +
				"        } \n" +
				"    }\n" +
				"    union\n" +
				"    {\n" +
				"        select \n" +
				"        distinct (?valid_until as ?ts) where {\n" +
				"            <<?s ?p ?o>> <https://github.com/GreenfishK/DataCitation/versioning/valid_until> ?valid_until.\n" +
				"        }\n" +
				"    }\n" +
				"}";
		return queryString;
	}

	public static String getVersionInfos_h() {
		String queryString = "Select (count(distinct ?ts) -1 as ?cnt_versions)\n" +
				" (min(?ts) as ?initial_version_ts) where {\n" +
				"    {\n" +
				"        select \n" +
				"        distinct (?valid_from as ?ts) where {\n" +
				"            <<<<?s ?p ?o>> <https://github.com/GreenfishK/DataCitation/versioning/valid_from> " +
				"?valid_from>>  <https://github.com/GreenfishK/DataCitation/versioning/valid_until> ?valid_until.\n" +
				"        } \n" +
				"    }\n" +
				"    union\n" +
				"    {\n" +
				"        select \n" +
				"        distinct (?valid_until as ?ts) where {\n" +
				"            <<<<?s ?p ?o>> <https://github.com/GreenfishK/DataCitation/versioning/valid_from> " +
				"?valid_from>>  <https://github.com/GreenfishK/DataCitation/versioning/valid_until> ?valid_until.\n" +
				"        }\n" +
				"    }\n" +
				"}";
		return queryString;
	}

}
