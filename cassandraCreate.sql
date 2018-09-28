--------------------------------
-- CASSANDRA KEYSPACE & TABLE --
--------------------------------

CREATE KEYSPACE IF NOT EXISTS "twitter" WITH REPLICATION = (
      'class': 'NetworkTopologyStrategy'
    , 'dataCenter1': 3
);

CREATE TABLE twitter.result (
      status_id                 bigint
    , status_created_at         bigint
    , user_id                   bigint
    , time_partition            bigint
    , in_reply_to_status_id     bigint
    , in_reply_to_user_id       bigint
    , user_created_at           bigint
    , user_statuses_count       bigint
    , user_favourites_count     bigint
    , user_followers_count      bigint
    , user_friends_count        bigint
    , in_reply_to_screen_name   text
    , user_screen_name          text
    , clean_text                text
    , status_text               text
    , rake_list                 LIST<text>
    , noun                      LIST<text>
    , preposition               LIST<text>
    , verb                      LIST<text>
    , clause                    LIST<text>
    , PRIMARY KEY (
        (status_id, time_partition)
        , status_created_at
    )
) WITH CLUSTERING ORDER BY (status_created_at DESC)
;


SELECT clean_text, in_reply_to_status_id
    FROM twitter.ss
    WHERE in_reply_to_status_id > 0
    ALLOW FILTERING
;


------------------------
-- CASSANDRA EXAMPLES --
------------------------

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS "twitter" WITH REPLICATION = {
      'class': 'NetworkTopologyStrategy',
    , 'DC1': 3 -- DataCenter specific replication factor
};

-- Create table
CREATE TABLE IF NOT EXISTS twitter.example (
      status_id       bigint
    , my_text         text
    , my_list         LIST<text>
    , my_dict         MAP<timestamp,text>
    , my_set          SET<double>
    , my_count        COUNTER
    , PRIMARY KEY     (status_id)
);

-- Use-case table
CREATE TABLE twitter.tt(
      status_id                 bigint
    , status_created_at         bigint
    , in_reply_to_status_id     bigint
    , PRIMARY KEY (
        ( status_id, time_partition ) -- PARTITION KEY
        , status_created_at           -- CLUSTER KEY
    )
) WITH CLUSTERING ORDER BY (status_created_at DESC);


-- Update example
UPDATE twitter.example (
    SET my_count = my_count + 1
    WHERE status_id = 123
);


------------------------
-- TOMBSTONE PROBLEMS --
------------------------

-- Do not do this unless working with a single node locally!
ALTER twitter.example WITH GC_GRACE_SECONDS = 0;

-- Do this!
# BASH
nodetool flush
nodetool compactionstats # displays pending compaction tasks
nodetool compact twitter # keyspace_name table_name


