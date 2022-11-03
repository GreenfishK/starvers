import re
import shutil

with open("/home/fkovacev/Uni/PhD/Semantic_Technologies/RDFVersioningEvaluation/rdfostrich_BEAR/playground/input.nt") as fin, open("tmp_out.ttl", "w") as fout:
    for line in fin:
        fout.write(line.replace(r'<extref href=\"http://opac.kent.ac.uk/cgi-bin/Pwebrecon.cgi?DB=local&PAGE=First', r'<extref href=\"http://opac.kent.ac.uk/cgi-bin/Pwebrecon.cgi?DB=local&amp;PAGE=First'))
fin.close()
fout.close()