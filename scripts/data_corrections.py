from pathlib import Path

# RDF_star flat
new_line1 = r'<<<http://dbpedia.org/resource/Rodeo_(Travis_Scott_album)> <http://dbpedia.org/property/cover> ' \
            r'"{\\rtf1\\ansi\\ansicpg1252{\\fonttbl}\n{\\colortbl;\\red255\\green255\\blue255;"@en >> ' \
            r'<https://github.com/GreenfishK/DataCitation/versioning/valid_from> ' \
            r'"2021-12-29T17:41:39.388+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> . '
new_line2 = r'<<<http://dbpedia.org/resource/Rodeo_(Travis_Scott_album)> <http://dbpedia.org/property/cover> ' \
            r'"{\\rtf1\\ansi\\ansicpg1252{\\fonttbl}\n{\\colortbl;\\red255\\green255\\blue255;"@en >> ' \
            r'<https://github.com/GreenfishK/DataCitation/versioning/valid_until> ' \
            r'"9999-12-31T12:00:00.000+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> . '

rdf_star_flat_in = str(Path.home()) + "/.BEAR/rawdata/bearb/hour/alldata.TB_star_flat.ttl"
rdf_star_flat_out = str(Path.home()) + "/.BEAR/rawdata/bearb/hour/alldata.TB_star_flat_out.ttl"
with open(rdf_star_flat_in) as fin, open(rdf_star_flat_out, "w") as fout:
    for line in rdf_star_flat_in:
        if r"\\\rtf1\\ansi\\ansicpg1252{\\" in line:
            line = new_line1 + "\n" + new_line2
            print(line)
        if r"{\\colortbl;\\\red255\\green255" in line:
            line = ""
            print(line)
        fout.write(line)
fin.close()
fout.close()

# RDF_star hierarchical
# TODO
new_line1 = r'<<<<<http://dbpedia.org/resource/Rodeo_(Travis_Scott_album)> <http://dbpedia.org/property/cover> ' \
            r'"{\\rtf1\\ansi\\ansicpg1252{\\fonttbl}\n{\\colortbl;\\red255\\green255\\blue255;"@en >> ' \
            r'<https://github.com/GreenfishK/DataCitation/versioning/valid_from> ' \
            r'"2021-12-29T17:41:39.388+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>>> ' \
            r'<https://github.com/GreenfishK/DataCitation/versioning/valid_until> ' \
            r'"9999-12-31T12:00:00.000+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> '

rdf_star_flat_in = str(Path.home()) + "/.BEAR/rawdata/bearb/hour/alldata.TB_star_flat.ttl"
rdf_star_flat_out = str(Path.home()) + "/.BEAR/rawdata/bearb/hour/alldata.TB_star_flat_out.ttl"
with open(rdf_star_flat_in) as fin, open(rdf_star_flat_out, "w") as fout:
    for line in rdf_star_flat_in:
        if r"\\\rtf1\\ansi\\ansicpg1252{\\" in line:
            line = new_line1 + "\n" + new_line2
            print(line)
        if r"{\\colortbl;\\\red255\\green255" in line:
            line = ""
            print(line)
        fout.write(line)
fin.close()
fout.close()
