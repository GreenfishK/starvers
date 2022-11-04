import re
import shutil
from rdflib import Graph

g = Graph()
fout_path = "/mnt/data/starvers_eval/rawdata/beara/alldata.IC.nt/corrected/1c.nt"
with open("/mnt/data/starvers_eval/rawdata/beara/alldata.IC.nt/1.nt") as fin, open(fout_path, "w") as fout:
    print("Correcting...")
    for i, line in enumerate(fin):
        fout = open(fout_path, "w")
        fout.write(line.replace(r' GMT+02:00"', r'+02:00"').replace(r'<extref href=\"http://opac.kent.ac.uk/cgi-bin/Pwebrecon.cgi?DB=local&PAGE=First', r'<extref href=\"http://opac.kent.ac.uk/cgi-bin/Pwebrecon.cgi?DB=local&amp;PAGE=First'))
        fout.close()
        print("Parsing line {0}: {1}".format(i, line))
        g.parse(fout_path)        
fin.close()



