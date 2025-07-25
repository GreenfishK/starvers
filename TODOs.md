# TODOs
TODO: Implement support for multiple RDF serializations including file archives and directories with multiple RDF files.

TODO: Investigate why import fails for for https://bimerr.iot.linkeddata.es/def/weather/ontology.nt

TODO: Implement client to pass a URL with an ontology behind to start the versioning

TODO: Implement stop versioning

TODO: Persist Postegresql Database

TODO: Fix bug: Wrongly packaed orkg_iterative files. The timestamps in the filenames in the zip archives do not reflect the timestamps of the zip archives

TODO: GUI
Distinguish between days were the service was not running and days when there were no inserts. Use colors or symbols to display this in the plot.

TODO: GUI
Add information about the inserts and deletes (metrics) in a side view or different tab:
* number of distinct classes
* number of added instances per class in the delta set
* number of instances with no class
* number of distinct object properties
* number of distinct data properties
* change ratio between two versions (BEAR)
    * $\delta_{i,j} = \frac{|\Delta⁺_{i,j} \cup \Delta⁻_{i,j}|}{|IC_i \cup IC_j|}$
    * $\delta⁺_{i,j} = \frac{|\Delta⁺_{i,j}|}{|IC_i|}$
    * $\delta⁻_{i,j} = \frac{|\Delta⁻_{i,j}|}{|IC_i|}$
    * positive and negative change ratios can be part of the tooltip/hovertemplate on individual bars on the insert/delete trace
* data growth  (BEAR)
    * $growth = \frac{|IC_j|}{|IC_i|}$
    * can be part of the tooltip/hovertemplate on individual points on the total triples trace
* static core (BEAR)
    * $C_A$ : take the first version and applying all the subsequent deletions.
* total version-oblivious triples  (BEAR)
    * $O_A$: number of different triples regardless of the timestamp

TODO: GUI
When hovering over the total triples data points (blue points) in the plot, the timestamp in the input field should take the value of the corresponding timestamp. 
    * For days and weeks the timestamp should be: yyyy-MM-ddT23:59:59, thereby taking the last minute of the day or week
    * For hours the timestamp should be: yyyy-MM-ddThh:mm:ss, copying the 


# Done
TODO: Resume service: If the repository exists, the tracking should be ressumed after a container shutdown or even deletion

TODO: timings_csv: add headers

TODO: Implement logs

TODO: A service that iterates through all zip files, 
calculates deltas between the consecutive snapshots 
and inserts them with the corresponding timestamp: -> retroVersioning service

TODO: 
* Add a new table ClassHierarchy to the database
    * dataset_id: foreign key
    * snapshot_id: foreign_key
    * snapshot_id_prev: foreign_key (non-identifying)
    * onto_class: done
    * parent_onto_class: done
    * cnt_class_instances_current: done
    * cnt_class_instances_prev: done
    * cnt_added_instances: done
    * cnt_deleted_instances: done

TODO: Feasability analysis: A SPARQL query that takes two 
versions and retrieves the class, parent class, number of total instances, 
number of new instances, is_new_class -> utils/graphdb/query_snapshot_metrics.sparql

