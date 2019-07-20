CREATE KEYSPACE analytics with REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE TABLE analytics.sensor_averages (
    sensor_uuid VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    temp_avg DECIMAL ,
    humidity_avg DECIMAL,
    max_radiation_level INT,
    PRIMARY KEY (sensor_uuid, start_time, end_time)
) with CLUSTERING ORDER BY (start_time DESC);

CREATE KEYSPACE sensor_analytics with REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

CREATE TABLE analytics.market_order_agg (
    sensor_uuid VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    temp_avg DECIMAL ,
    humidity_avg DECIMAL,
    max_radiation_level INT,
    PRIMARY KEY (sensor_uuid, start_time, end_time)
) with CLUSTERING ORDER BY (start_time DESC);