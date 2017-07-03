#include <iostream>
#include <HDTManager.hpp>
#include <stdio.h>
#include <stdlib.h>     /* atoi */
#include <sstream>
#include <getopt.h>
#include <iostream>
#include <fstream>
#include "../src/util/StopWatch.hpp"

using namespace std;
using namespace hdt;

void help() {
	cout << "$ versionQuery [options] <hdtfile> " << endl;
	cout << "\t-h\t\t\tThis help" << endl;
	cout << "\t-d\t<dir>\t\tdirectory with the HDT versions" << endl;
	cout << "\t-i\t<query>\t\tLaunch query and exit." << endl;
	//cout << "\t-o\t<output>\tSave query output to file." << endl;
	cout << "\t-t\t<type>\t\ttype of query [s, p, o]." << endl;
	cout << "\t-o\t<output>\tSave query output to file." << endl;
	cout << "\t-l\t<number>\t\tlimit upt to <number> of versions" << endl;
	//cout << "\t-v\tVerbose output" << endl;
}
vector<string> split(const string& str, const string& delim) {
	vector<string> tokens;
	size_t prev = 0, pos = 0;
	do {
		pos = str.find(delim, prev);
		if (pos == string::npos)
			pos = str.length();
		string token = str.substr(prev, pos - prev);
		if (!token.empty())
			tokens.push_back(token);
		prev = pos + delim.length();
	} while (pos < str.length() && prev < str.length());
	return tokens;
}
int main(int argc, char *argv[]) {

	int c;
	string inputFile, outputFile, limit;
	string type = "null";
	string dir = "data/hdt/";
	while ((c = getopt(argc, argv, "hi:t:l:o:d:")) != -1) {
		switch (c) {
		case 'h':
			help();
			break;
		case 'd':
			dir = optarg;
			break;
		case 'i':
			inputFile = optarg;
			break;
		case 'o':
			outputFile = optarg;
			break;
		case 't':
			type = optarg;
			break;
		case 'l':
			limit = optarg;
			break;
		default:
			cout << "ERROR: Unknown option" << endl;
			help();
			return 1;
		}
	}
	// Load HDT file
	vector<HDT*> HDTversions;

	int numVersions = 0;
	if (limit.length() > 0) {
		numVersions = atoi((char*) limit.c_str());
	} else {
		cerr << "[WARNING] limit not provided, trying to load 58 versions"
				<< endl;
		numVersions = 58;
	}
	ostream *out;
	ofstream outF;

	if (outputFile != "") {
		outF.open(outputFile.c_str());
		out = &outF;
	} else {
		out = &cout;
	}

	for (int i = 0; i < numVersions; i++) {
		std::stringstream sstm;
		sstm << dir << (i + 1) << ".hdt";
		cout << "Loading " << sstm.str() << endl;
		HDTversions.push_back(
				HDTManager::mapIndexedHDT((char*) sstm.str().c_str()));
		//cout<<"loaded 1.hdt! Press any key to load 2.hdt"<<endl;
		//int c = getchar();

	}

	cout << "WARMUP... " << endl;
	for (int i = 0; i < numVersions; i++) {
		// Enumerate all different predicates
		cout << "Dataset " << (i + 1) << " contains "
				<< HDTversions[i]->getDictionary()->getNpredicates()
				<< " predicates." << endl;

		// Enumerate all triples matching a pattern ("" means any)
		IteratorTripleString *it = HDTversions[i]->search("", "", "");
		int count = 0;
		while (it->hasNext() && count < 100) {
			TripleString *triple = it->next();
			//cout << "Result Warmup: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
			count++;
		}
		delete it; // Remember to delete iterator to avoid memory leaks!

		/*IteratorUCharString *itPred = HDTversions[i]->getDictionary()->getPredicates();
		 while(itPred->hasNext()) {
		 unsigned char *str = itPred->next(); // Warning this pointer is only valid until next call to next();
		 cout << str << endl;
		 itPred->freeStr(str);
		 }
		 delete itPred;  // Remember to delete iterator to avoid memory leaks!
		 */
	}
	cout << "... WARMUP finished!" << endl;

	if (type == "null") {
		cerr << "[ERROR] Please provide a type of query (-t [s,p,o])" << endl;
		help();
		exit(0);
	}

	//read queries
	cout << "opening file:" << inputFile << endl;
	std::ifstream file((char*) inputFile.c_str());
	cout << "opened! " << endl;

	if (!file.good())
		throw "unable to open filter file";
	string linea = "";

	double totalTime = 0;
	int num_queries = 0;
	while (!file.eof()) {
		getline(file, linea);
		cout << "Reading line:" << linea << endl;

		if (linea.length() == 0)
			continue;
		size_t pos = linea.find(' ');

		if (pos != std::string::npos) {
			string query = linea.substr(0, pos);
			string subject = "", predicate = "", object = "";
			if (type == "s") {
				subject = query;
			} else if (type == "p") {
				predicate = query;
			} else if (type == "o") {
				object = query;
			} else {
				vector<string> elements = split(linea, " ");
				if (type == "sp") {
					subject = elements[0];
					predicate = elements[1];
				} else if (type == "so") {
					subject = elements[0];
					object = elements[1];
				} else if (type == "po") {
					predicate = elements[0];
					object = elements[1];
				} else if (type == "spo") {
					subject = elements[0];
					predicate = elements[1];
					object = elements[2];
				}
			}

			StopWatch st;
			int numResults = 0;
			//this should be in parallel but we do it sequential because is very fast so far
			for (int i = 0; i < numVersions; i++) {

				IteratorTripleString *it = HDTversions[i]->search(
						subject.c_str(), predicate.c_str(), object.c_str());

				while (it->hasNext()) {
					TripleString *triple = it->next();
					//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
					numResults++;
				}
				delete it;

			}
			double time = st.stopReal() / 1000000;
			totalTime += time;
			cout << numResults << " results in " << time << " ms" << endl;
			num_queries++;
		}
	}
	//compute mean of queries
	*out << "<Queries>,<mean_time>,<total>" << endl;
	*out << num_queries << "," << totalTime / num_queries <<","<<totalTime<< endl;

	for (int i = 0; i < numVersions; i++) {
		delete HDTversions[i]; // Remember to delete instance when no longer needed!
	}
	if (outputFile != "") {
		outF.close();
	}
}
