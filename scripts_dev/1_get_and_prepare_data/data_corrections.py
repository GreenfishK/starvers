from datetime import timedelta, datetime
from pathlib import Path
from rdflib import Graph
import shutil
import re


def correct_bearb_hour(policy: str, file: str, init_ts: datetime):
    """
    Bug 1: If either the alldata.TB_star_flat.ttl or alldata.TB_star_hierarchical.ttl dataset was constructed
    from computed .nt change sets there might be occurrences of faulty escapes, such as \\b, \\f and \\r in
    the alldata.TB_star_flat.ttl file. Triples, where such escapes occur are replaced by the corresponding
    correct triples from the original change sets. This bug only occurs in version 93.
    """
    print("Correct bearb_hour TBSF and TBSH datasets: bad IRI with special characters")

    version_ts = init_ts + timedelta(seconds=92)
    sys_ts_formatted = datetime.strftime(version_ts, "%Y-%m-%dT%H:%M:%S.%f")[:-3]
    xsd_datetime = "<http://www.w3.org/2001/XMLSchema#dateTime>"
    tz_offset = "+02:00"
    rdf_version_ts_res = '"{ts}{tz_offset}"^^{datetimeref}'.format(ts=sys_ts_formatted, tz_offset=tz_offset,
                                                                   datetimeref=xsd_datetime)
    if policy == "tbsf":

        new_line1 = r'<<<http://dbpedia.org/resource/Rodeo_(Travis_Scott_album)> <http://dbpedia.org/property/cover> ' \
                    r'"{\\rtf1\\ansi\\ansicpg1252{\\fonttbl}\n{\\colortbl;\\red255\\green255\\blue255;"@en >> ' \
                    r'<https://github.com/GreenfishK/DataCitation/versioning/valid_from> ' \
                    + rdf_version_ts_res + r' . '
        new_line2 = r'<<<http://dbpedia.org/resource/Rodeo_(Travis_Scott_album)> <http://dbpedia.org/property/cover> ' \
                    r'"{\\rtf1\\ansi\\ansicpg1252{\\fonttbl}\n{\\colortbl;\\red255\\green255\\blue255;"@en >> ' \
                    r'<https://github.com/GreenfishK/DataCitation/versioning/valid_until> ' \
                    r'"9999-12-31T00:00:00.000+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> . '

        rdf_star_flat_in = file
        with open(rdf_star_flat_in) as fin, open("tmp_out.ttl", "w") as fout:
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
        shutil.move("tmp_out.ttl", file)

    if policy == "tbsf":
        new_line1 = r'<<<<<http://dbpedia.org/resource/Rodeo_(Travis_Scott_album)> <http://dbpedia.org/property/cover> ' \
                    r'"{\\rtf1\\ansi\\ansicpg1252{\\fonttbl}\n{\\colortbl;\\red255\\green255\\blue255;"@en >> ' \
                    r'<https://github.com/GreenfishK/DataCitation/versioning/valid_from> ' \
                    + rdf_version_ts_res + r'>> ' \
                    r'<https://github.com/GreenfishK/DataCitation/versioning/valid_until> ' \
                    r'"9999-12-31T00:00:00.000+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> .'

        rdf_star_hierarchical_in = file
        with open(rdf_star_hierarchical_in) as fin, open("tmp_out.ttl", "w") as fout:
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
        shutil.move("tmp_out.ttl", file)


def correct_bearc(policy: str, file: str):
    """
    Bad IRI found in BEARC dataset which prevents the Jena TDB2 loader to load it.
    Bad IRI: http:/cordis.europa.eu/data/cordis-fp7projects-xml.zip
    Manual check: with query:
        select * where { 
        ?s ?p ?o.
        filter (regex (str(?o), "http:/(?!/).*"))
        } 
    """

    if policy == "ic":
        print("Correct bearc IC datasets: bad IRI")

        bad_IRI = r'<http:/cordis.europa.eu/data/cordis-fp7projects-xml.zip>' 
        correct_IRI = r'<http://cordis.europa.eu/data/cordis-fp7projects-xml.zip>' 

        snapshot = file
        with open(snapshot) as fin, open("tmp_out.ttl", "w") as fout:
            for line in fin:
                fout.write(line.replace(bad_IRI, correct_IRI))
        fin.close()
        fout.close()
        shutil.move("tmp_out.ttl", file)

def correct_beara(policy: str, file: str):
    """
    Bad datatime format found in BEARA dataset which prevents rdflib's Graph() constructor to load it.
    Bad IRI: http:/cordis.europa.eu/data/cordis-fp7projects-xml.zip
    Looking for other bad IRIs:
        select * where { 
        ?s ?p ?o.
        filter (regex (str(?o), "http:/(?!/).*"))
        } 
    None found.
    """

    if policy == "ic":
        print("Correct beara IC datasets: format datetime")
        
        bad_datatime_format= r' GMT+02:00"' 
        correct_datatime_format = r'+02:00"' 

        snapshot = file
        with open(snapshot) as fin, open("tmp_out.ttl", "w") as fout:
            for line in fin:
                fout.write(line.replace(bad_datatime_format, correct_datatime_format))
        fin.close()
        fout.close()
        shutil.move("tmp_out.ttl", file)
        
        print("Correct beara IC datasets: escape ampersands")

        snapshot = file
        strIn = open(snapshot, "r").read()
        pattern = r'&(?!amp;)'
        strOut = re.sub(pattern, r'&amp;', strIn)
        fout = open(r"tmp_out.ttl", "w")
        fout.write(strOut)
        fout.close()
        shutil.move("tmp_out.ttl", file)
