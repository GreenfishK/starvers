from pathlib import Path


def correct(dataset: str, source: str, destination: str):

    if dataset == "rdf_star_flat":
        """
        Bug 1: If the alldata.TB_star_flat.ttl dataset was constructed from computed .nt change sets there
        might be occurrences of faulty escapes, such as \\b, \\f and \\r in the alldata.TB_star_flat.ttl file.
        Triples, where such escapes occur are replaced by the corresponding correct triples from the original change sets.
        """
        new_line1 = r'<<<http://dbpedia.org/resource/Rodeo_(Travis_Scott_album)> <http://dbpedia.org/property/cover> ' \
                    r'"{\\rtf1\\ansi\\ansicpg1252{\\fonttbl}\n{\\colortbl;\\red255\\green255\\blue255;"@en >> ' \
                    r'<https://github.com/GreenfishK/DataCitation/versioning/valid_from> ' \
                    r'"2021-12-29T17:41:39.388+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> . '
        new_line2 = r'<<<http://dbpedia.org/resource/Rodeo_(Travis_Scott_album)> <http://dbpedia.org/property/cover> ' \
                    r'"{\\rtf1\\ansi\\ansicpg1252{\\fonttbl}\n{\\colortbl;\\red255\\green255\\blue255;"@en >> ' \
                    r'<https://github.com/GreenfishK/DataCitation/versioning/valid_until> ' \
                    r'"9999-12-31T00:00:00.000+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> . '

        rdf_star_flat_in = source
        rdf_star_flat_out = destination
        with open(rdf_star_flat_in) as fin, open(rdf_star_flat_out, "w") as fout:
            for line in fin:
                if r"\\\rtf1\\ansi\\ansicpg1252{\\" in line:
                    line = new_line1 + "\n" + new_line2
                    print(line)
                if r"{\\colortbl;\\\red255\\green255" in line:
                    line = ""
                    print(line)
                fout.write(line)
        fin.close()
        fout.close()

    if dataset == "rdf_star_hierarchical":
        """
        Bug 1: If the alldata.TB_star_hierarchical.ttl dataset was constructed from computed .nt change sets there
        might be occurrences of faulty escapes, such as \\b, \\f and \\r in the alldata.TB_star_flat.ttl file.
        Triples, where such escapes occur are replaced by the corresponding correct triples from the original change sets.
        """
        new_line1 = r'<<<<<http://dbpedia.org/resource/Rodeo_(Travis_Scott_album)> <http://dbpedia.org/property/cover> ' \
                    r'"{\\rtf1\\ansi\\ansicpg1252{\\fonttbl}\n{\\colortbl;\\red255\\green255\\blue255;"@en >> ' \
                    r'<https://github.com/GreenfishK/DataCitation/versioning/valid_from> ' \
                    r'"2021-12-29T17:41:39.388+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>>> ' \
                    r'<https://github.com/GreenfishK/DataCitation/versioning/valid_until> ' \
                    r'"9999-12-31T00:00:00.000+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .'

        rdf_star_hierarchical_in = source
        rdf_star_hierarchical_out = destination
        with open(rdf_star_hierarchical_in) as fin, open(rdf_star_hierarchical_out, "w") as fout:
            for line in fin:
                if r"\\\rtf1\\ansi\\ansicpg1252{\\" in line:
                    line = new_line1 + "\n"
                    print(line)
                if r"{\\colortbl;\\\red255\\green255" in line:
                    line = ""
                    print(line)
                fout.write(line)
        fin.close()
        fout.close()


out_frm = "ttl"
data_dir = str(Path.home()) + "/.BEAR/rawdata/bearb/hour"
correct(dataset="rdf_star_hierarchical",
        source=data_dir + "/alldata.TB_star_hierarchical." + out_frm,
        destination=data_dir + "/alldata.TB_star_hierarchical_out." + out_frm)