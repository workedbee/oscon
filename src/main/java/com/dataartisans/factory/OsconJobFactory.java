package com.dataartisans.factory;

import com.dataartisans.data.DataPoint;
import com.dataartisans.data.KeyedDataPoint;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Factory produces sources and sinks
 */
public interface OsconJobFactory {
    SinkFunction<KeyedDataPoint<Double>> createSink(String name);
    SourceFunction<DataPoint<Long>> createSource();
}
