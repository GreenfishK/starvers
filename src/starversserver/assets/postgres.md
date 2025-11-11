# Various DB updates during development
psql -d starvers_db -U user -W

update dataset set repository_name = 'schema_org_ontology' where repository_name 
= 'schema_org_ontology_v2';
update dataset set repository_name = 'air_quality_ontology' where repository_name 
= 'air_quality_ontology_v2';
update dataset set repository_name = 'orkg' where repository_name 
= 'orkg_v2';

truncate snapshot;

update dataset set active = False;
update dataset set active = True;

update dataset set active = True where repository_name = 'schema_org_ontology';
update dataset set active = True where repository_name = 'orkg';
update dataset set active = True where repository_name = 'schema_org_ontology' or repository_name = 'air_quality_ontology';

update dataset set next_run = null;
update dataset set next_run = '2025-08-04 16:35:11.064284';

delete from dataset where repository_name = 'schema_org_ontology_iterative' and repository_name = 'air_quality_ontology_iterative' and repository_name = 'orkg_iterative';

select * from snapshot where dataset_id = '1ede0112-ee5e-4b56-88bb-e76904e9e929' and snapshot_ts = '2025-05-08 08:06:13.736';

update snapshot set parent_onto_class = NULL 
where parent_onto_class = 'NaN' 
and dataset_id = '831764af-25d9-4830-b653-9780e69ed53e';

update snapshot set parent_onto_class = NULL 
where parent_onto_class = 'NaN' 
and dataset_id = '55c4c558-9643-46b4-8f19-24a74b670708';

select dataset_id, snapshot_ts, cnt_class_instances_current, cnt_class_instances_prev from snapshot where dataset_id = '32a0d2ce-b65b-4a5c-9d5d-39815e035969' 
and abs(cnt_class_instances_current - cnt_class_instances_prev) > 0;

select distinct b.snapshot_ts, a.repository_name, a.next_run, count(b.onto_class), count(b.onto_property) from dataset a join snapshot b on a.id = b.dataset_id where a.repository_name = 'schema_org_ontology' group by b.snapshot_ts, a.repository_name, a.next_run  order by snapshot_ts desc;


# Create tables and insert data
CREATE TABLE dataset (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    repository_name TEXT NOT NULL,
    rdf_dataset_url TEXT NOT NULL,
    polling_interval INTEGER NOT NULL,
    notification_webhook TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_modified TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    active BOOLEAN DEFAULT TRUE,
    next_run TIMESTAMP WITHOUT TIME ZONE,
    cnt_triples_static_core INTEGER,
    cnt_triples_version_oblivious INTEGER,
    ratio_avg_data_growth DOUBLE PRECISION,
    ratio_avg_change DOUBLE PRECISION
);

CREATE TABLE snapshot (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL REFERENCES dataset(id),
    snapshot_ts TIMESTAMP WITHOUT TIME ZONE,
    snapshot_ts_prev TIMESTAMP WITHOUT TIME ZONE,
    onto_class TEXT,
    onto_class_label TEXT,
    parent_onto_class TEXT,
    cnt_class_instances_current INTEGER,
    cnt_class_instances_prev INTEGER,
    cnt_classes_added INTEGER,
    cnt_classes_deleted INTEGER,
    onto_property TEXT,
    onto_property_label TEXT,
    parent_property TEXT,
    cnt_property_instances_current INTEGER,
    cnt_property_instances_prev INTEGER,
    cnt_properties_added INTEGER,
    cnt_properties_deleted INTEGER,
    ratio_data_growth DOUBLE PRECISION,
    ratio_change DOUBLE PRECISION
);

INSERT INTO dataset (
    name,
    repository_name,
    rdf_dataset_url,
    polling_interval,
    notification_webhook,
    created_at,
    last_modified,
    active,
    id,
    next_run
) VALUES
(
    'The Open Research Knowledge Graph',
    'orkg',
    'https://orkg.org/files/rdf-dumps/rdf-export-orkg.nt',
    86400,
    NULL,
    '2025-05-25 16:36:01.378723',
    '2025-07-27 16:42:56.267383',
    TRUE,
    '831764af-25d9-4830-b653-9780e69ed53e',
    '2025-08-01 16:36:55.611705'
),
(
    'schema.org Ontology',
    'schema_org_ontology',
    'https://schema.org/version/latest/schemaorg-current-https.nt',
    86400,
    NULL,
    '2025-05-25 16:35:27.376885',
    '2025-07-28 16:58:36.792085',
    TRUE,
    '55c4c558-9643-46b4-8f19-24a74b670708',
    '2025-08-01 16:36:55.611705'
),
(
    'Air Quality Data for a City Ontology',
    'air_quality_ontology',
    'https://vocab.linkeddata.es/datosabiertos/def/medio-ambiente/calidad-aire/ontology.nt',
    86400,
    NULL,
    '2025-05-25 16:35:02.298197',
    '2025-07-28 16:58:50.793766',
    TRUE,
    '7c0606ab-dc16-49d3-929a-3e6ea8eba410',
    '2025-08-01 16:36:55.611705'
),
(
    'Animals and Plants Test Ontology',
    'test',
    'http://example.com',
    86400,
    NULL,
    '2025-05-25 16:35:02.298197',
    '2025-07-28 16:58:50.793766',
    TRUE,
    'f2d3c6e1-72b6-4e4f-96c6-2a8f4f61e9d7',
    '2025-08-01 16:36:55.611705'
),
(
    'SI Digital Framework',
    'si',
    'https://raw.githubusercontent.com/TheBIPM/SI_Digital_Framework/refs/heads/main/SI_Reference_Point/TTL/si.ttl',
    86400,
    NULL,
    '2025-11-11 10:05:36.792085',
    '2025-11-11 10:05:36.792085',
    TRUE,
    '4a30eec7-59ca-4a2a-8e49-cfbb47276a5f',
    '2025-11-11 11:05:36.792085'
);


# Alter table
alter table dataset add column cnt_triples_static_core INTEGER; 
alter table dataset add column cnt_triples_version_oblivious INTEGER; 
alter table dataset add column ratio_avg_data_growth DOUBLE PRECISION;
alter table dataset add column ratio_avg_change DOUBLE PRECISION;
alter table snapshot add column ratio_data_growth DOUBLE PRECISION;
alter table snapshot add column ratio_change DOUBLE PRECISION;

alter table snapshot add column onto_property TEXT;
alter table snapshot add column parent_property TEXT;
alter table snapshot add column cnt_property_instances_current INTEGER;
alter table snapshot add column cnt_property_instances_prev INTEGER;
alter table snapshot add column cnt_properties_added INTEGER;
alter table snapshot add column cnt_properties_deleted INTEGER;

alter table snapshot add column onto_property_label TEXT;
alter table snapshot add column onto_class_label TEXT;
