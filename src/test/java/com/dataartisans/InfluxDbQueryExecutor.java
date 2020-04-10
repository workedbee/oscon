package com.dataartisans;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import static com.dataartisans.sinks.InfluxDbConstants.*;

/**
 * Influx database connection to execute queries
 */
public class InfluxDbQueryExecutor {
    private InfluxDB influxDB = null;

    public InfluxDbQueryExecutor() {
        connect();
    }

    public void connect() {
        influxDB = InfluxDBFactory.connect(DEFAULT_URL, DEFAULT_LOGIN, DEFAULT_PASSWORD);
        influxDB.createDatabase(DATABASE_NAME);
    }

    public QueryResult query(Query query) {
        return influxDB.query(query);
    }
}
