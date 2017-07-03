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
	cout << "$ diffJump5_CB [options] <hdtfile> " << endl;
	cout << "\t-h\t\t\tThis help" << endl;
	cout << "\t-i\t<query>\t\tLaunch query and exit." << endl;
	cout << "\t-d\t<dir>\t\tdirectory with the HDT versions" << endl;
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
	string dir = "data/diffPolicyHDT/";
	while ((c = getopt(argc, argv, "hi:d:t:l:o:")) != -1) {
		switch (c) {
		case 'h':
			help();
			return 1;
		case 'i':
			inputFile = optarg;
			break;
		case 'd':
			dir = optarg;
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

	vector<double> times(numVersions, 0);
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

			int jump = 1;
			int totalIterations = ((numVersions - 1) / jump) + 1; //-1 because we start in 0
			for (int i = 0; i < totalIterations; i++) {
				int versionQuery = 1; //always compare against the first version=0, then we compare the changes with the next one, version=1
				int postversionQuery = min((i + 1) * jump, numVersions) - 1;
				cout << "diff between " << versionQuery-1 << " "
						<< postversionQuery << endl;

				//this should be in parallel but we do it sequential because is very fast so far
				StopWatch st;

				set<string> results_add;
				set<string> results_del;
				int numResults = 0;
				for (int j = versionQuery; j <= postversionQuery; j++) {
					IteratorTripleString* it_add = HDTversions_add[j]->search(
							subject.c_str(), predicate.c_str(), object.c_str());

					while (it_add->hasNext()) {
						TripleString* triple = it_add->next();
						//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
						string tripleString = triple->getSubject() + " "
								+ triple->getPredicate() + " "
								+ triple->getObject();
						if (results_del.erase(tripleString) != 1) {
							results_add.insert(tripleString);
							numResults++;

						}
					}
					delete it_add;
					//FIXME patch error on empty HDTs
					//if (j != 0 && j != 30) {
					if (j != 0) {
						IteratorTripleString* it_del =
								HDTversions_del[j]->search(subject.c_str(),
										predicate.c_str(), object.c_str());

						while (it_del->hasNext()) {
							TripleString* tripleDel = it_del->next();
							//cout << "Result: " << triple->getSubject() << ", " << triple->getPredicate() << ", " << triple->getObject() << endl;
							string tripleDelString = tripleDel->getSubject()
									+ " " + tripleDel->getPredicate() + " "
									+ tripleDel->getObject();
							if (results_add.erase(tripleDelString) != 1) {
								results_del.insert(tripleDelString);
								numResults--;
							}
						}
						delete it_del;
					}
				}
				int adds = 0;
				int dels = 0;
				//cout<<"results1 size:"<<results1.size()<<endl;
				//cout<<"results2 size:"<<results2.size()<<endl;
				for (std::set<string>::iterator it = results_add.begin();
						it != results_add.end(); ++it) {
					adds++;
				}
				for (std::set<string>::iterator it = results_del.begin();
						it != results_del.end(); ++it) {
					dels++;
				}

				double time = st.stopReal() / 1000000;
				times[i] = times[i] + time;
				cout << "-- " << adds << " adds and " << dels << " dels, in "
						<< time << " ms" << endl;

			}

			num_queries++;

		}

	}
	cout << "compute mean of queries" << endl;
	fflush(stdout);
	//compute mean of queries
	*out << "<version>,<mean_time>,<total>" << endl;
	for (int i = 0; i < numVersions; i++) {
		*out << (i + 1) << "," << times[i] / num_queries <<","<<times[i] << endl;
	}

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
