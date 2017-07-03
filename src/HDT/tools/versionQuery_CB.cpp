#include <iostream>
#include <HDTManager.hpp>
#include <stdio.h>
#include <stdlib.h>     /* atoi */
#include <sstream>
#include <getopt.h>
#include <iostream>
#include <fstream>
#include "../src/util/StopWatch.hpp"
#include "../src/hdt/BasicHDT.hpp"
#include <HDT.hpp>

using namespace std;
using namespace hdt;

void help() {
	cout << "$ matVersion_CB [options] <hdtfile> " << endl;
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
int verQuery(int numVersions, const vector<HDT*>& HDTversions_add,
		const string& subject, const string& predicate, const string& object,
		const vector<HDT*>& HDTversions_del) {
	set<string> results;
	//this should be in parallel but we do it sequential because is very fast so far

	int numResults = 0;

	for (int i = 0; i < numVersions; i++) {
		//cout << "search add in i=" << i << endl;
		IteratorTripleString* it_add = HDTversions_add[i]->search(
				subject.c_str(), predicate.c_str(), object.c_str());

		while (it_add->hasNext()) {
			TripleString* triple = it_add->next();
			//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
			results.insert(
					triple->getSubject() + " " + triple->getPredicate() + " "
							+ triple->getObject());

		}
		//cout << numResults << " results inserted" << endl;
		delete it_add;
		//cout << "search del in i=" << i << endl;
		//FIXME patch error on empty HDTs
		//if (i != 0 && i != 30) {
		if (i != 0) {
			IteratorTripleString* it_del = HDTversions_del[i]->search(
					subject.c_str(), predicate.c_str(), object.c_str());

			while (it_del->hasNext()) {
				TripleString* tripledel = it_del->next();
				//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
				int erased = results.erase(
						tripledel->getSubject() + " "
								+ tripledel->getPredicate() + " "
								+ tripledel->getObject());
				//cout << "Erased:" << erased << endl;

			}
			delete it_del;
		}

		//iterate to show the results at version i
		for (std::set<string>::iterator it = results.begin();
				it != results.end(); ++it) {
			numResults++;
		}
		//cout << numResults << " results after deletion" << endl;
	}

	return numResults;
}

int main(int argc, char *argv[]) {

	int c;
	string inputFile, outputFile, limit;
	string type = "null";
	string dir = "data/diffPolicyHDT/";
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
	vector<HDT*> HDTversions_add;
	vector<HDT*> HDTversions_del;

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
		sstm << dir << i << ".add.hdt";
		cout << "Loading " << sstm.str() << endl;
		HDTversions_add.push_back(
				HDTManager::mapIndexedHDT((char*) sstm.str().c_str()));
		//cout<<"loaded 1.hdt! Press any key to load 2.hdt"<<endl;
		//int c = getchar();

	}
	for (int i = 0; i < numVersions; i++) {
		std::stringstream sstm;
		sstm << dir << i << ".del.hdt";
		cout << "Loading " << sstm.str() << endl;
		//FIXME patch error on empty HDTs
		//if (i == 0 || i == 30) {
		if (i == 0) {
			HDTversions_del.push_back(NULL);
		} else {
			HDTversions_del.push_back(
					HDTManager::mapIndexedHDT((char*) sstm.str().c_str()));
		}
		//cout<<"loaded 1.hdt! Press any key to load 2.hdt"<<endl;
		//int c = getchar();

	}

	cout << "WARMUP... " << endl;
	for (int i = 0; i < numVersions; i++) {
		// Enumerate all different predicates
		cout << "Dataset " << (i + 1) << " contains "
				<< HDTversions_add[i]->getDictionary()->getNpredicates()
				<< " predicates." << endl;

		// Enumerate all triples matching a pattern ("" means any)
		IteratorTripleString *it = HDTversions_add[i]->search("", "", "");
		int count = 0;
		while (it->hasNext() && count < 100) {
			TripleString *triple = it->next();
			//cout << "Result Warmup: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
			count++;
		}
		delete it; // Remember to delete iterator to avoid memory leaks!
	}
	for (int i = 0; i < numVersions; i++) {
		//FIXME patch error on empty HDTs
		//if (i != 0 && i != 30) {
		if (i != 0) {
			HDTversions_del.push_back(NULL);

			// Enumerate all different predicates
			cout << "Dataset " << (i + 1) << " contains "
					<< HDTversions_del[i]->getDictionary()->getNpredicates()
					<< " predicates." << endl;

			// Enumerate all triples matching a pattern ("" means any)
			IteratorTripleString *it = HDTversions_del[i]->search("", "", "");
			int count = 0;
			while (it->hasNext() && count < 100) {
				TripleString *triple = it->next();
				//cout << "Result Warmup: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
				count++;
			}
			delete it; // Remember to delete iterator to avoid memory leaks!
		}
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
			int numResults = 0;
			StopWatch st;

			numResults += verQuery(numVersions, HDTversions_add, subject,
					predicate, object, HDTversions_del);

			double time = st.stopReal() / 1000000;
			cout << numResults << " results in " << time << " ms" << endl;
			totalTime += time;
			num_queries++;

		}

	}
	//compute mean of queries
	*out << "<Queries>,<mean_time>,<total>" << endl;
	*out << num_queries << "," << totalTime / num_queries<<","<<totalTime<< endl;

	for (int i = 0; i < numVersions; i++) {
		delete HDTversions_add[i]; // Remember to delete instance when no longer needed!
		//FIXME patch error on empty HDTs
		//if (i != 0 && i != 30) {
		if (i != 0) {
			delete HDTversions_del[i]; // Remember to delete instance when no longer needed!
		}
	}
	if (outputFile != "") {
		outF.close();
	}
}
