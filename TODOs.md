# TODOs
## Features
TODO: Implement support for multiple RDF serializations including file archives and directories with multiple RDF files.

TODO: Investigate why import fails for for https://bimerr.iot.linkeddata.es/def/weather/ontology.nt

TODO: GUI
* Distinguish between days were the service was not running and days when there were no inserts. Use colors or symbols to display this in the plot.

TODO: GUI
Add information about the inserts and deletes (metrics) of a snapshot in a side view or different tab:
* number of total classes
* number of total instance
* number of total, added, and deleted instances per class: done
* number of instances with no class
* number of total object properties
* number of total data properties

TODO: GUI, DatasetModel
Add information about the whole dataset in the left side section.
* static core (BEAR)
    * $C_A$ (in BEAR paper): take the first version and applying all the subsequent deletions. Count the triples
    * $C_A$ (in Starvers): query all triples that have an artificial end timestamp and a creation timestamp from the first snapshot.
    * Add a new field to the Dataset "cnt_triples_static"
    * Update the field after every versioning task
* total version-oblivious triples (BEAR)
    * $O_A$: Number of different triples regardless of the timestamp: Query data triples (inner-most triples) that have a start and end timestamp and count the distinct triples
    * Add a new field to the Dataset "cnt_triples_version_oblivious"
    * Update the field after every versioning task
* average data growth
    * $growth = \sum_{i=0, j = i+1}^N\frac{|IC_j|}{|IC_i|}$
    * $avg(growth)$
    * Add a new field to the Snapshot "ratio_data_growth"
    * Add a new field to the Dataset "ratio_avg_data_growth"
    * Update the field after every versioning task
* average change ratio
    * $\delta_{i,j} = \frac{|\Delta⁺_{i,j} \cup \Delta⁻_{i,j}|}{|IC_i \cup IC_j|}$
    * $\delta⁺_{i,j} = \frac{|\Delta⁺_{i,j}|}{|IC_i|}$
    * $\delta⁻_{i,j} = \frac{|\Delta⁻_{i,j}|}{|IC_i|}$
    * $1/N \times \sum_{i=0, j=i+1}^N\delta_{i,j}$
    * Add a new field to the Snapshot "ratio_change"
    * Add a new field to the Dataset "ratio_avg_change"

TODO: GUI: Update view of timestamp of x-axis
* HOUR view: Hours, day, month, and year should be shown in a 4 layer view to avoid repeating the day, month, and year for every hour
* DAY view: Year, month, and day should be shown in a 3-layer view to avoid repeating the month and year for every day
* Week view: like day view

TODO: GUI, controller: In addition to hour, day, and week, add a view for actual snapshot intervals

TODO: Consider adding a trace (curve) for change ratios between snapshots. Note: Change ratios can be computed between individual snapshots but one has to be careful when aggregating ratios, e.g. for a day or week, as they can overstate the change. For example, if a triple was added between v1 and v2 and then deleted in v3 again within one day, the net result is no change but the change ratio for v1_v2 and v2_v3 would be be > 0.

TODO: Add an "summarize" feature that generates a summary of the changes between two versions.

TODO: Backend: When user leaves session, send a query abort to the Triple Store.

TODO: GUI: Add a sorting option for the class and property hierarchy that sorts them either according to the instance count or change count (added + deleted triples)

TODO: Add a feature that lets the user select two data points in the plot and the changes between these two versions are shown.

## Bug fixes
TODO: GUI: Fix bug with tooltip

TODO: Metrics calculation: Fix bug with calculation for added and deleted triples. The bug is visible for schema.org when the property changes 
for the 08.09.2025 and the 09.09.2025 are compared. the isPartOf property
has a wrong number of total instances: on the 09.09.2025. What probably went 
wrong is that the calculation for the 09.09.2025 for some reason used a 
wrong reference date, i.e. the same as the calculation on the 08.9.2025.



# Done
fix bug with week aggregation. the shown number of total triples in one week datapoint does not correspond to the same date in the "day view"

Implement client to pass a URL with an ontology behind to start the versioning

Persist Postegresql Database

GUI
When selecting one data point (blue points) in the plot, the timestamp in the input field should take the value of the corresponding timestamp. 
    * For days and weeks the timestamp should be: yyyy-MM-ddT23:59:59, thereby taking the last minute of the day or week
    * For hours the timestamp should be: yyyy-MM-ddThh:mm:ss, the same as in the data point

Resume service: If the repository exists, the tracking should be ressumed after a container shutdown or even deletion

timings_csv: add headers

Implement logs

A service that iterates through all zip files, 
calculates deltas between the consecutive snapshots 
and inserts them with the corresponding timestamp: -> compute service

Add a new table ClassHierarchy to the database
    * dataset_id: foreign key
    * snapshot_id: foreign_key
    * snapshot_id_prev: foreign_key (non-identifying)
    * onto_class: done
    * parent_onto_class: done
    * cnt_class_instances_current: done
    * cnt_class_instances_prev: done
    * cnt_added_instances: done
    * cnt_deleted_instances: done

Feasability analysis: A SPARQL query that takes two 
versions and retrieves the class, parent class, number of total instances, 
number of new instances, is_new_class -> utils/graphdb/query_snapshot_classes.sparql

