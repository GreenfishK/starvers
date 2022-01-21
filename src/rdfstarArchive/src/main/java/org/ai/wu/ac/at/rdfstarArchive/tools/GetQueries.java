package org.ai.wu.ac.at.rdfstarArchive.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class GetQueries {

	private static Options options;
	private static Integer noOfURIs;

	private static CommandLine parseOption(String[] args) {
		options = new Options();
		options.addOption("h", "help", false, "show help.");
		options.addOption("i",true, "the statistic file");
		options.addOption("e", true,"epsilon value");
		options.addOption("o", true,"output directory");
		options.addOption("s", true,"number of maximum URLs");
		options.addOption("p", true,"pattern of the query");
		
		CommandLineParser parser = new BasicParser();
		try {
			CommandLine cmd = parser.parse( options, args);
			
			if(args.length<8 ||cmd.hasOption("h") || cmd.hasOption("help")) help();
			return cmd;
		} catch (ParseException e1) {
			System.err.println( "Parsing failed.  Reason: " + e1.getMessage() );
			help(); return null;
		}
	}
	private static void help() {
		HelpFormatter formater = new HelpFormatter();
		formater.printHelp("Main", options);
		System.exit(0);
	}
	
	

	
	public static void main(String[] args) throws FileNotFoundException {
//		args = new String[]{"-i","empty/p/all.stats",
//				"-o","empty/p/",
////				"-e","0.15",
//				"-s","10"};
		CommandLine cmd = parseOption(args);

				
		String input = cmd.getOptionValue("i");
		String output = cmd.getOptionValue("o");
		String pattern = "any";
		if (cmd.hasOption("p")){
			pattern = cmd.getOptionValue("p");
		}
		Double e = Double.valueOf(cmd.getOptionValue("e"));
		noOfURIs = Integer.valueOf(cmd.getOptionValue("s"));
	
		System.out.println("Selecting urls with range of  e="+e);
		
		Map<Integer, Set<String>> bins = new HashMap<Integer, Set<String>>();
		
		Scanner s = new Scanner(new File(input));
		while(s.hasNextLine()){
			String line = s.nextLine();
			String [] t = line.trim().split(" ");
			System.out.println("line:"+line);
			if(t.length<9) continue;
			
			Double min =0.0;
			Double mean =0.0;
			Double max =0.0;
			
			if (pattern.equalsIgnoreCase("any")){
				min = Double.valueOf(t[1]);
				mean = Double.valueOf(t[2]);
				max = Double.valueOf(t[3]);
			}
			else if (pattern.equalsIgnoreCase("sp")|| pattern.equalsIgnoreCase("so")||pattern.equalsIgnoreCase("po")){
				// includes another element in the middle
				min = Double.valueOf(t[2]);
				mean = Double.valueOf(t[3]);
				max = Double.valueOf(t[4]);
			}
			else if (pattern.equalsIgnoreCase("spo")){
				// includes another element in the middle
				min = Double.valueOf(t[3]);
				mean = Double.valueOf(t[4]);
				max = Double.valueOf(t[5]);
			}
			System.out.println("min:"+min);
			System.out.println("mean:"+mean);
			System.out.println("max:"+max);
			
			if (min == 0) continue;
			
			double  tmin = mean * (1-e),
					tmax=mean*(1+e); 
			
			if( (min >= tmin) && (max <= tmax)){
				
				int [] a = {1,10,50,100,250,500,1000,1500,2000};
				for(int i:a){
					if(mean <= i){
						getSet(bins,i).add(line);
						break;
					}
				}
			}else{
				System.out.println(line);
				System.out.println("["+min+", "+max+"] not in range ["+tmin+", "+tmax+"]");
			}
		}
		File  out = new File(output,"queries");
		out.mkdirs();
		for(Entry<Integer, Set<String>> ent: bins.entrySet()){
			
			PrintWriter pw = new PrintWriter(new File(out,"queries-all-"+ent.getKey()+"-e"+e+".txt"));
			PrintWriter pw1 = new PrintWriter(new File(out,"queries-sel-"+ent.getKey()+"-e"+e+".txt"));
			
			
			HashSet<String> domains = new HashSet<String>();
			HashSet<String> ignored = new HashSet<String>();
			Integer c =0;
			for(String st: ent.getValue()){
				c = sample(st, domains, ignored, pw1,c);
				pw.println(st);
			}
			while(c < noOfURIs && ignored.size()>0){
				HashSet<String> domains1 = new HashSet<String>();
				HashSet<String> ignored1 = new HashSet<String>();
				for(String st: ignored)
					c=sample(st, domains1, ignored1, pw1,c);
				ignored= ignored1;
			}
			System.out.println("bin-"+ent.getKey()+" "+ent.getValue().size()+" sampled:"+c);
			pw.close();
			pw1.close();
		}
		
	}

	private static int sample(String st, HashSet<String> domains,
			HashSet<String> ignored, PrintWriter pw1, int c) {
		try {
			if(c < noOfURIs){
				//parse URIs for the sampling
				String uri=st.trim().split(" ")[0].replaceAll("<",	"").replaceAll(">",	"");
				String domain = new URL(uri).getAuthority();
				if(!domains.contains(domain)){
					pw1.println(st);
					domains.add(domain);
					c++;
				}else{ignored.add(st);}
			}
		} catch (MalformedURLException e1) {
			System.out.println("Malfromed URL "+e1);
		}
		return c;
		
	}
	private static Set<String> getSet(Map<Integer, Set<String>> bins, int i) {
		Set<String>set=bins.get(i);
		if(set==null){
			set = new HashSet<String>();
			bins.put(i, set);
		}
		return set;
	}
}