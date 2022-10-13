package org.ai.wu.ac.at.rdfstarArchive.solutions;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;

/**
 * @author Javier D. Fernández
 *
 */
public class DiffSolution {

	Entry<ArrayList<String>, ArrayList<String>> diff;

	public DiffSolution(Entry<ArrayList<String>, ArrayList<String>> diff) {
		super();
		this.diff = diff;
	}
	public DiffSolution(ArrayList<String> adds, ArrayList<String> dels) {
		diff = new AbstractMap.SimpleEntry<ArrayList<String>, ArrayList<String>>(adds, dels);
		
	}
	public DiffSolution(HashSet<String> adds, HashSet<String> dels) {
		diff = new AbstractMap.SimpleEntry<ArrayList<String>, ArrayList<String>>(new ArrayList<String>(adds), new ArrayList<String>(dels));
		
	}
	public ArrayList<String> getAdds(){
		return diff.getKey();
	}
	public ArrayList<String> getDels(){
		return diff.getValue();
	}
	
	
}
