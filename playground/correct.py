import re
import shutil

with open("/home/fkovacev/Uni/PhD/Semantic_Technologies/RDFVersioningEvaluation/rdfostrich_BEAR/playground/input.nt") as fin, open("tmp_out.ttl", "w") as fout:
    for line in fin:
        fout.write(re.sub(r'&(?!amp;)', r'&amp;', line))
fin.close()
fout.close()