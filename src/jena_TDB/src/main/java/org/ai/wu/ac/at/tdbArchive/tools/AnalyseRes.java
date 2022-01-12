package org.ai.wu.ac.at.tdbArchive.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.zip.GZIPInputStream;

import org.ai.wu.ac.at.tdbArchive.api.JenaTDBArchive;
import org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_IC;
import org.ai.wu.ac.at.tdbArchive.utils.QueryUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;


public class AnalyseRes {

	public static void main(String[] args) throws ParseException, IOException, InterruptedException, ExecutionException {
		// TODO Auto-generated method stub
		Options options = new Options();
		Option dirOpt = new Option("d",true, "directory for tdb files");
		dirOpt.setRequired(true);
		options.addOption(dirOpt);
		Option file = new Option("f", true,"list of URLs");
		file.setRequired(true);
		options.addOption(file);
		Option output = new Option ("o", true,"output directory");
		output.setRequired(true);
		options.addOption(output);
		Option values = new Option ("t", true,"allowed values subject,predicate,object ");
		values.setRequired(true);
		options.addOption(values);
		Option versions = new Option ("v", true,"number of versions");
		versions.setRequired(true);
		options.addOption(versions);
		
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse( options, args);
		
		String tdbFolder = cmd.getOptionValue("d");
		File resFile = new File(cmd.getOptionValue("f"));
		String type = cmd.getOptionValue("t");
		String numVersions = cmd.getOptionValue("v");
		File outDir = new File(cmd.getOptionValue("o"),type);
		
		if(!outDir.exists()) outDir.mkdirs();
		
		long start = System.currentTimeMillis();
		JenaTDBArchive jenaArchive = new JenaTDBArchive_IC();
		jenaArchive.load(tdbFolder, null);
		
		System.out.println("Loaded "+tdbFolder+" in "+(System.currentTimeMillis()-start)+" ms");
		
		InputStream is = new FileInputStream(resFile);
		if(resFile.getName().endsWith(".gz")){
			is = new GZIPInputStream(is);
		}
		Scanner s = new Scanner(is);
		
		while(s.hasNextLine()){
			String line = s.nextLine();
					
			String[] uri = line.trim().split(" ");
//			System.out.println("|"+uri[0]+"|"+numVersions+"|"+(uri[0]==numVersions)+(uri[0].equalsIgnoreCase(numVersions)) );
//			System.out.println("|"+uri[1]+"|");
//			System.out.println("|"+uri.length+"|"+(uri.length==2));
//			System.out.println("|"+numVersions+"|");
			
			if(uri.length>=2 && uri[0].equalsIgnoreCase(numVersions) ){ //present in all versions (uri[0]==58)
				System.out.println(uri[0]+" "+uri[1]);
				String[] TPquery = {"","",""};
				TPquery[0] = uri[1];
				if (uri.length>2){
					TPquery[1]=uri[2];
				}
				if (uri.length>3){
					TPquery[2]=uri[3];
				}
				String query = QueryUtils.createLookupQuery(type, TPquery);
				Map<Integer,ArrayList<String>> solutions = new HashMap<Integer, ArrayList<String>>();
				for (int i=0;i<Integer.parseInt(numVersions);i++){
					ArrayList<String> solution = jenaArchive.matQuery(i, query);
					System.out.println(" Solutions for version "+i+":"+solution.size());
					solutions.put(i, solution);
				}
				
				analyseQuery(solutions, outDir,uri[1]);
				
			}
		}
		s.close();
		
		
		
		
//		QueryExecution qe = QueryExecutionFactory.create(query, dataset) ;
	}




	private static void analyseQuery(Map<Integer, ArrayList<String>> solutions, File outDir, String uri) throws IOException {
		 DescriptiveStatistics _dynStats;
		DescriptiveStatistics _cardStats;

		_dynStats= new DescriptiveStatistics();
		_cardStats= new DescriptiveStatistics();
		
		_cardStats.addValue(solutions.get(0).size());
		TreeMap<Integer, Double>_dyn= new TreeMap<Integer, Double>();
		for(int i=0; i<solutions.size()-1; i++){
			_cardStats.addValue(solutions.get(i+1).size());
			
			ArrayList<String> a = solutions.get(i);
			ArrayList<String> b = solutions.get(i+1);
			if(a.size()==0){
				System.out.println("WARN: empty results for snapshot "+i);
			}if(b.size()==0){
				System.out.println("WARN: empty results for snapshot "+(i+1));
			}
			
			double dyn=0;
			if(a.size() >= b.size()){ 
				dyn=compare(b,a);
			}else{ 
				dyn=compare(a,b);
			}
			
			if(dyn == Double.NaN){
				System.out.println("WARN: NAN for "+i+"-"+(i+1));
			}else{
				_dyn.put(i, dyn);
			}
			
//			System.out.println("____________________");
//			if(dyn>0){
//				for(QuerySolution s: a){
//					System.out.println((i)+" "+s);
//					
//				}for(QuerySolution s: b){
//					System.out.println((i+1)+" "+s+ " "+a.contains(s));
//				}
//			}
//			System.out.println(dyn);
			_dynStats.addValue(dyn);
		}
		
		File dir = new File(outDir,"data");
		dir.mkdirs();
		PrintWriter pw = new PrintWriter(new File(dir, URLEncoder.encode(uri.replaceAll("<", "").replaceAll(">", ""))+".stats"));
		pw.println("1 "+solutions.get(1).size()+" 0.0");
		
		for(int i=1; i<solutions.size()-1; i++){
			pw.println(i+" "+solutions.get(i+1).size()+" "+_dyn.get(i+1));
		}
		pw.close();
		FileWriter fw = new FileWriter(new File(outDir, "all.stats"),true);
		fw.write(uri+" "+
				_cardStats.getMin()+" "+
				_cardStats.getMean()+" "+
				_cardStats.getMax()+" "+
				_cardStats.getStandardDeviation()+" "+
				_dynStats.getMin()+" "+
				_dynStats.getMean()+" "+
				_dynStats.getMax()+" "+
				_dynStats.getStandardDeviation()+"\n"
				);
		fw.close();
		
	}

	private static  double compare(ArrayList<String> b, ArrayList<String> a) {
		int total = b.size()+a.size();
		int same=0;
		for(String sol: b){
			if(a.contains(sol)){
				same++;
			}
		}
		double d = (a.size()-same)+(b.size()-same);
		return (d/total);
	}

	
	

}