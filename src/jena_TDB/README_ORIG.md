# Compile
- mvn install

This will create a jar (target/tdbQuery-0.6-jar-with-dependencies.jar) with all the Jena depencies included. 
# Run Queries

 - java  -cp target/tdbQuery-0.6-jar-with-dependencies.jar org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query
 
Please consider to increase the Java heap with the flag *-XmxSIZE*, e.g. "java *-Xmx64G* -cp ..." to use a maximum of 64GB RAM memory.

## Usage:

| Argument      | Result       |
| ------------- |-------------|
|-a,--allVersionQueries `<arg>`  | Dynamic queries to process in all versions|
| -c,--category `<arg>`          | Query category: mat &#124; diff &#124; ver &#124; change|
| -d,--dir `<arg>`               | DIR to load TDBs|
| -e,--endversion `<arg>`        |Version end, used in the Query (e.g. in diff)|
| -h,--help                      |Shows help|
| -j,--jump `<arg>`              |Jump step for the diff: e.g. 5 (0-5,0-10..)|
| -o,--OutputResults `<arg>`     | Output file with Results|
| -p,--policy `<arg>`            | Policy implementation: ic &#124; cb &#124; tb &#124; cbtb &#124; hybrid|
| -q,--query `<arg>`             | Single SPARQL query to process, applied on -v version|
| -Q,--MultipleQueries `<arg>`   |File with several SPARQL queries|
| -r,--rol `<arg>`               | Rol of the Resource in the query: subject (s) &#124; predicate (p) &#124; object (o)|
| -S,--SplitResults              | Split Results by version (creates one file per version)|
| -s,--silent                    | Silent output, that is, don't show results|
| -t,--timeOutput `<arg>`        | File to write the time output|
| -v,--version `<arg>`           | Version, used in the Query (e.g. in materialize)|


