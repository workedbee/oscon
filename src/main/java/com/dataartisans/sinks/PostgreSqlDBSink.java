package com.dataartisans.sinks;

import com.dataartisans.data.DataPoint;
import com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import static com.dataartisans.sinks.PostgreSqlDbConstants.*;

/**
 * Database sink based on PostgreSQL implementation
 */
public class PostgreSqlDBSink<T extends DataPoint<Double>> extends RichSinkFunction<T> {
    private final String measurement;
    private Connection connection;

    public PostgreSqlDBSink(String measurement){
        this.measurement = measurement;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            connection = DriverManager.getConnection(DEFAULT_DRIVER_URL, DEFAULT_LOGIN, DEFAULT_PASSWORD);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        createDefaultTables(connection);
    }

    @Override
    public void close() throws Exception {
        super.close();

        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Override
    public void invoke(T dataPoint, Context context) {
        try {
            String key =  dataPoint instanceof KeyedDataPoint
                    ? ((KeyedDataPoint) dataPoint).getKey()
                    : "unknown";

            String query = String.format("INSERT INTO %s (created, key, value) VALUES(TO_TIMESTAMP(?/1000), ?, ?)",
                    measurement);
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setLong(1, dataPoint.getTimeStampMs());
            preparedStatement.setString(2, key);
            preparedStatement.setDouble(3, dataPoint.getValue());

            preparedStatement.execute();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void createDefaultTables(Connection connection) {
        try {
            String SQL_SELECT = String.format("CREATE TABLE IF NOT EXISTS %s (\n" +
                    "    id SERIAL PRIMARY KEY,\n" +
                    "    created timestamp NOT NULL,\n" +
                    "    key VARCHAR(128) NOT NULL,\n" +
                    "    value double precision NOT NULL)", measurement);
            PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT);
            preparedStatement.execute();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
