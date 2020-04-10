package com.dataartisans.factory;

import com.dataartisans.data.DataPoint;
import com.dataartisans.data.KeyedDataPoint;
import com.dataartisans.sinks.InfluxDBSink;
import com.dataartisans.sources.TimestampSource;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 *
 */
public class DefaultOsconJobFactory implements OsconJobFactory {
    private static final int SLOWDOWN_FACTOR = 1;
    private static final int PERIOD_MS = 100;

    @Override
    public SinkFunction<KeyedDataPoint<Double>> createSink(String name) {
        return new InfluxDBSink<>(name);
    }

    @Override
    public SourceFunction<DataPoint<Long>> createSource() {
        return new TimestampSource(PERIOD_MS, SLOWDOWN_FACTOR);
    }
}
