package com.dataartisans.jobs;

import com.dataartisans.data.DataPoint;
import com.dataartisans.data.KeyedDataPoint;
import com.dataartisans.factory.DefaultOsconJobFactory;
import com.dataartisans.factory.OsconJobFactory;
import com.dataartisans.functions.AssignKeyFunction;
import com.dataartisans.functions.SawtoothFunction;
import com.dataartisans.functions.SineWaveFunction;
import com.dataartisans.functions.SquareWaveFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class OsconJob {

  public static void main(String[] args) throws Exception {
    mainWithFactory(new DefaultOsconJobFactory());
  }

  public static void mainWithFactory(OsconJobFactory factory) throws Exception {
//    Configuration conf = new Configuration();
//    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
//    final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

    // set up the execution environment
    final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();

//    env.enableCheckpointing(1000);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // Simulate some sensor data
    DataStream<KeyedDataPoint<Double>> sensorStream = generateSensorData(env, factory);

    // Write this sensor stream out to InfluxDB
    sensorStream
      .addSink(factory.createSink("sensors"))
      .name("sensors-sink");

    // Compute a windowed sum over this data and write that to InfluxDB as well.
    sensorStream
      .keyBy("key")
      .timeWindow(Time.seconds(1))
      .sum("value")
      .name("window")
      .addSink(factory.createSink("summedSensors"))
      .name("summed-sensors-sink");

    // add a socket source
//    KeyedStream<ControlMessage, Tuple> controlStream = env.socketTextStream("localhost", 9999)
//      .map(msg -> ControlMessage.fromString(msg))
//      .keyBy("key");
//
//    // modulate sensor stream via control stream
//    sensorStream
//      .keyBy("key")
//      .connect(controlStream)
//      .flatMap(new AmplifierFunction())
//      .addSink(new InfluxDBSink<>("amplifiedSensors"));

    // execute program
    env.execute("Flink Demo");
  }

  private static DataStream<KeyedDataPoint<Double>> generateSensorData(StreamExecutionEnvironment env, OsconJobFactory factory) {
    // boiler plate for this demo
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1000, 1000));
    env.setMaxParallelism(8);
    env.setParallelism(1);
    env.disableOperatorChaining();
    env.getConfig().setLatencyTrackingInterval(1000);

    // Initial data - just timestamped messages
    DataStreamSource<DataPoint<Long>> timestampSource =
      env.addSource(factory.createSource(), "test data");

    // Transform into sawtooth pattern
    SingleOutputStreamOperator<DataPoint<Double>> sawtoothStream = timestampSource
      .map(new SawtoothFunction(10))
      .name("sawTooth");

    // Simulate temp sensor
    SingleOutputStreamOperator<KeyedDataPoint<Double>> tempStream = sawtoothStream
      .map(new AssignKeyFunction("temp"))
      .name("assignKey(temp)");

    // Make sine wave and use for pressure sensor
    SingleOutputStreamOperator<KeyedDataPoint<Double>> pressureStream = sawtoothStream
      .map(new SineWaveFunction())
      .name("sineWave")
      .map(new AssignKeyFunction("pressure"))
      .name("assignKey(pressure");

    // Make square wave and use for door sensor
    SingleOutputStreamOperator<KeyedDataPoint<Double>> doorStream = sawtoothStream
      .map(new SquareWaveFunction())
      .name("squareWave")
      .map(new AssignKeyFunction("door"))
      .name("assignKey(door)");

    // Combine all the streams into one and write it to Kafka
    DataStream<KeyedDataPoint<Double>> sensorStream =
      tempStream
        .union(pressureStream)
        .union(doorStream);

    return sensorStream;
  }

}
