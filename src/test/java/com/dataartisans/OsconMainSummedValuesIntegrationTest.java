package com.dataartisans;

import com.dataartisans.data.DataPoint;
import com.dataartisans.data.KeyedDataPoint;
import com.dataartisans.factory.OsconJobFactory;
import com.dataartisans.jobs.OsconJob;
import com.dataartisans.sinks.InfluxDBSink;
import com.dataartisans.sinks.InfluxDbConstants;
import com.dataartisans.sinks.PostgreSqlDBSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;

/**
 * Integration tests to check correct Oscon work with:
 *  - SummedValues,
 *  - PostgreSQL implementation of database sink.
 */
public class OsconMainSummedValuesIntegrationTest {
    private static final int FLINK_WORK_DURATION_MS = 5000;

    @Test
    public void testOnInfluxDB() throws Exception {
        final long startTime = System.currentTimeMillis();

        executeOsconMainOnInflux(FLINK_WORK_DURATION_MS);

        List<Double> values = getSummedValuesFromInfluxDb(startTime);

        checkSummedValues(values);
    }

    @Test
    public void testOnPostgreSqlDB() throws Exception {
        final long startTime = System.currentTimeMillis();

        executeOsconMainOnPostgreSql(FLINK_WORK_DURATION_MS);

        List<Double> values = getSummedValuesFromPostgreSql(startTime);

        checkSummedValues(values);
    }

    private void executeOsconMainOnInflux(int durationMs) throws Exception {
        OsconJob.mainWithFactory(new OsconJobFactory() {
            @Override
            public SinkFunction<KeyedDataPoint<Double>> createSink(String name) {
                return new InfluxDBSink<>(name);
            }

            @Override
            public SourceFunction<DataPoint<Long>> createSource() {
                return new LimitedTimestampSource(durationMs);
            }
        });
    }

    private void executeOsconMainOnPostgreSql(int durationMs) throws Exception {
        OsconJob.mainWithFactory(new OsconJobFactory() {
            @Override
            public SinkFunction<KeyedDataPoint<Double>> createSink(String name) {
                return new PostgreSqlDBSink<>(name);
            }

            @Override
            public SourceFunction<DataPoint<Long>> createSource() {
                return new LimitedTimestampSource(durationMs);
            }
        });
    }

    private List<Double> getSummedValuesFromInfluxDb(long startTime) {
        InfluxDbQueryExecutor executor = new InfluxDbQueryExecutor();

        String whereClause = String.format("time > now() - %dms AND \"key\"='pressure'",
                System.currentTimeMillis() - startTime);
        QueryResult resultSet = executor.query(
                new Query(
                        buildInfluxQuery("summedSensors", whereClause),
                        InfluxDbConstants.DATABASE_NAME));

        return resultSet.getResults().stream()
                .flatMap(res -> res.getSeries().stream())
                .flatMap(series -> series.getValues().stream())
                .map(entry -> (Double)entry.get(1) )
                .collect(Collectors.toList());
    }

    private List<Double> getSummedValuesFromPostgreSql(long startTime) {
        try {
            PostgreSqlDbQueryExecutor executor = new PostgreSqlDbQueryExecutor();

            String query = String.format("SELECT created, key, value FROM summedSensors " +
                            "WHERE created > TO_TIMESTAMP(%d/1000) AND key = 'pressure'",
                    startTime);

            ResultSet resultSet = executor.query(query);

            List<Double> result = new ArrayList<>();
            while (resultSet.next()) {
                Double value = resultSet.getDouble("value");
                result.add(value);
            }
            return result;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private void checkSummedValues(List<Double> values) {
        // Last value is often not 0.0 since generation can stop in any point in time (not at the end of period)
        if (!values.isEmpty() && (values.get(values.size() - 1) != 0.0)) {
            values.remove(values.size() - 1);
        }

        boolean nonZeroPresent = values.stream()
                .anyMatch(value -> value != 0.);
        assertFalse("Summed measurements have value != 0.0", nonZeroPresent);

        assertFalse("No summed measurements with value = 0.0", values.isEmpty());
    }

    private String buildInfluxQuery(String series, String whereClause) {
        return String.format("SELECT \"value\" FROM %s WHERE %s", series, whereClause);
    }
}