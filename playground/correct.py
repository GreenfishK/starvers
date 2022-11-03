import re
import shutil

strIn = open("/home/fkovacev/Uni/PhD/Semantic_Technologies/RDFVersioningEvaluation/rdfostrich_BEAR/playground/input.nt", "r").read()
pattern = r'&(?!amp;)'
strOut = re.sub(pattern, r'&amp;', strIn)
fout = open(r"tmp_out.ttl", "w")
fout.write(strOut)
fout.close()
shutil.move("tmp_out.ttl", file)
