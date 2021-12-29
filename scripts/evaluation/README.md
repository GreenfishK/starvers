# Jena TDB
Run the following command to execute the queries for the timestamp-based policies (tb, tb_star_h, tb_star_f) and the materialization query type (mat)
```
./run-docker-tdb.sh bearb-hour 2>&1 | tee bearb-hour.log
```
Upon execution following new directories and files are created (marked with *):

```
home/.BEAR/  
.
.
.
└── output  
    └── *time
        └── *tdb
	    └──*bearb-hour
	       └── <hostname><system-timestamp>
	       	   └── time-tb-mat-lookup_queries_p.txt.csv
		   └── time-tb-mat-lookup_queries_po.txt.csv
		   └── time-tb_star_h-mat-lookup_queries_p.txt.csv
		   └── time-tb_star_h-mat-lookup_queries_po.txt.csv
		   └── time-tb_star_f-mat-lookup_queries_p.txt.csv
		   └── time-tb_star_f-mat-lookup_queries_po.txt.csv

```
# HDT
Not evaluated yet.
