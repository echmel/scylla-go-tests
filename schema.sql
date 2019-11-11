CREATE KEYSPACE dmp_db with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TYPE dmp_db.segment (
    source text,
    probability tinyint,
    recency timestamp,
    expired_at timestamp,
    value text
    );

CREATE TABLE dmp_db.segments
(
    dmp_id      UUID PRIMARY KEY,
    segment_set map<int, frozen<segment>> -- A map of text keys, and text values
);
