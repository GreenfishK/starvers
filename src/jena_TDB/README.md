Run the following command to execute the queries for all policies (TB, CB, IC) and query types (mat, diff, ver):
./run-docker.sh bearb-hour 2>&1 | tee bearb-day.log

We get a warning that the logger is not properly initialized

```
log4j:WARN No appenders could be found for logger (org.apache.jena.riot.system.stream.JenaIOEnvironment).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
```
Most of the queries could be executed but some fail due to a java.lang.OutOfMemoryError exception (stack trace see below):

| Policy      | Query category | Query set     | error/no error |
| :---        |    :----:   |  :----:   |       ---: |
|  IC | MAT | lookup_queries_p.txt  | ![#f03c15](https://via.placeholder.com/15/f03c15/000000?text=+) |
| CB | MAT | lookup_queries_p.txt | ![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| TB | MAT | lookup_queries_p.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| IC | VER  |lookup_queries_p.txt | ![#f03c15](https://via.placeholder.com/15/f03c15/000000?text=+) |
| CB | VER | lookup_queries_p.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| TB | VER | lookup_queries_p.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| IC| DIFF  | lookup_queries_p.txt  | ![#f03c15](https://via.placeholder.com/15/f03c15/000000?text=+) |
| CB | DIFF | lookup_queries_p.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| TB | DIFF | lookup_queries_p.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| IC | MAT | lookup_queries_po.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| CB | MAT | lookup_queries_po.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| TB | MAT | lookup_queries_po.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| IC | VER | lookup_queries_po.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| CB | VER | lookup_queries_po.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| TB | VER | lookup_queries_po.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| IC | DIFF | lookup_queries_po.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| CB | DIFF | lookup_queries_po.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |
| TB | DIFF | lookup_queries_po.txt |![#c5f015](https://via.placeholder.com/15/c5f015/000000?text=+) |

```
Exception in thread "main" java.lang.OutOfMemoryError: GC overhead limit exceeded
	at org.apache.jena.atlas.lib.DS.list(DS.java:42)
	at org.apache.jena.atlas.iterator.IteratorConcat.<init>(IteratorConcat.java:34)
	at org.apache.jena.atlas.iterator.IteratorConcat.concat(IteratorConcat.java:51)
	at org.apache.jena.sparql.engine.binding.BindingBase.vars(BindingBase.java:76)
	at org.apache.jena.sparql.engine.binding.BindingProjectBase.actualVars(BindingProjectBase.java:63)
	at org.apache.jena.sparql.engine.binding.BindingProjectBase.vars1(BindingProjectBase.java:57)
	at org.apache.jena.sparql.engine.binding.BindingBase.vars(BindingBase.java:74)
	at org.apache.jena.sparql.engine.binding.BindingFactory.materialize(BindingFactory.java:55)
	at org.apache.jena.tdb.solver.QueryEngineTDB$QueryIteratorMaterializeBinding.moveToNextBinding(QueryEngineTDB.java:131)
	at org.apache.jena.sparql.engine.iterator.QueryIteratorBase.nextBinding(QueryIteratorBase.java:153)
	at org.apache.jena.sparql.engine.iterator.QueryIteratorWrapper.moveToNextBinding(QueryIteratorWrapper.java:42)
	at org.apache.jena.sparql.engine.iterator.QueryIteratorBase.nextBinding(QueryIteratorBase.java:153)
	at org.apache.jena.sparql.engine.iterator.QueryIteratorBase.next(QueryIteratorBase.java:128)
	at org.apache.jena.sparql.engine.iterator.QueryIteratorBase.next(QueryIteratorBase.java:40)
	at org.apache.jena.sparql.engine.ResultSetStream.nextBinding(ResultSetStream.java:86)
	at org.apache.jena.sparql.engine.ResultSetStream.nextSolution(ResultSetStream.java:114)
	at org.apache.jena.sparql.engine.ResultSetStream.next(ResultSetStream.java:123)
	at org.apache.jena.sparql.engine.ResultSetCheckCondition.next(ResultSetCheckCondition.java:65)
	at org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_IC.warmup(JenaTDBArchive_IC.java:820)
	at org.ai.wu.ac.at.tdbArchive.core.JenaTDBArchive_IC.bulkAllMatQuerying(JenaTDBArchive_IC.java:209)
	at org.ai.wu.ac.at.tdbArchive.tools.JenaTDBArchive_query.main(JenaTDBArchive_query.java:242)
```
