package it.consulthink.oe.readers;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.DateSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;

import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import it.consulthink.oe.db.InfluxDB2Sink;
import it.consulthink.oe.db.InfluxDBSink;
import it.consulthink.oe.model.NMAJSONData;
import it.consulthink.oe.model.TotalTraffic;

/*
 * At a high level, TotalTrafficReader reads from a Pravega stream, and prints
 * the packet count summary to the output. This class provides an example for
 * a simple Flink application that reads streaming data from Pravega.
 *
 * And  after flink transformation  output redirect to another pravega stream.
 *
 * This application has the following input parameters
 *     stream - Pravega stream name to read from
 *     controller - the Pravega controller URI, e.g., tcp://localhost:9090
 *                  Note that this parameter is automatically used by the PravegaConfig class
 */
public class TotalTrafficReaderToInflux extends AbstractApp {

	// Logger initialization
	private static final Logger LOG = LoggerFactory.getLogger(TotalTrafficReaderToInflux.class);

	// The application reads data from specified Pravega stream and once every 10
	// seconds
	// prints the distinct words and counts from the previous 10 seconds.
	public TotalTrafficReaderToInflux(AppConfiguration appConfiguration) {
		super(appConfiguration);
	}

	public static BoundedOutOfOrdernessTimestampExtractor<TotalTraffic> getTimestampAndWatermarkAssigner() {
		BoundedOutOfOrdernessTimestampExtractor<TotalTraffic> timestampAndWatermarkAssigner = new BoundedOutOfOrdernessTimestampExtractor<TotalTraffic>(
				Time.seconds(1)) {

			@Override
			public long extractTimestamp(TotalTraffic element) {
				return element.getTime().getTime();
			}

		};
		return timestampAndWatermarkAssigner;
	}

	public void run() {
		LOG.info("Run " + this.getClass().getName() + "...");

		try {
			initializeFlinkStreaming();
		} catch (Exception e) {
			LOG.error("Error on initializeFlinkStreaming", e);
			throw new RuntimeException("Error on initializeFlinkStreaming", e);
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		AppConfiguration.StreamConfig inputStreamConfig = appConfiguration.getInputStreamConfig();

		String inputStreamName = inputStreamConfig.getStream().getStreamName();
		createStream(inputStreamConfig);
		LOG.info("============== input stream  =============== " + inputStreamName);

		// Create EventStreamClientFactory
		PravegaConfig pravegaConfig = appConfiguration.getPravegaConfig();

		SourceFunction<TotalTraffic> sourceFunction = getSourceFunction(pravegaConfig, inputStreamName);

		DataStream<TotalTraffic> source = env.addSource(sourceFunction).name("Pravega." + inputStreamName);

		BoundedOutOfOrdernessTimestampExtractor<TotalTraffic> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();


		String influxdbUrl = appConfiguration.getInfluxdbUrl();

		String influxdbVersion = appConfiguration.getInfluxdbVersion();

		String influxdbUsername = appConfiguration.getInfluxdbUsername();
		String influxdbPassword = appConfiguration.getInfluxdbPassword();
		String influxdbDbName = appConfiguration.getInfluxdbDb();

		String org = appConfiguration.getInfluxdbOrg();
		String token = appConfiguration.getInfluxdbToken();
		String bucket = appConfiguration.getInfluxdbBucket();

		RichSinkFunction<TotalTraffic> sink = null;

		if (influxdbVersion.equals("1")) {
			sink = getSink1(inputStreamName, influxdbUrl, influxdbUsername, influxdbPassword, influxdbDbName);
		} else {
			sink = getSink2(inputStreamName, influxdbUrl, org, token, bucket);
		}
		
		DataStreamSink<TotalTraffic> dataStream = source
				.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
//				.keyBy(t -> {
//					return t.getTime();
//				})
//				.window(TumblingEventTimeWindows.of(Time.seconds(1)))
//				.reduce(new ReduceFunction<TotalTraffic>() {
//
//					@Override
//					public TotalTraffic reduce(TotalTraffic value1, TotalTraffic value2)
//							throws Exception {
//						LOG.info("Reduce " + value1 + " " + value2);
//						if (value1.getTime().equals(value2.getTime())) {
//							return new TotalTraffic(value1.getTime(), value1.getValue() + value2.getValue());
//						} else {
//							LOG.error("Error Reduce " + value1 + " " + value2);
//							throw new RuntimeException("Different Time on same Key");
//						}
//
//					}
//				})
				.addSink(sink).name("Influx." + inputStreamName);

		
		// execute within the Flink environment
		try {
			env.execute(this.getClass().getName());
		} catch (Exception e) {
			LOG.error("Error executing " + this.getClass().getName() + "...", e);
			throw new RuntimeException("Error executing " + this.getClass().getName() + "...", e);
		} finally {
			LOG.info("Finally execute " + this.getClass().getName() + "...");
		}

	}

	public static RichSinkFunction<TotalTraffic> getSink1(String inputStreamName, String influxdbUrl,
			String influxdbUsername, String influxdbPassword, String influxdbDbName) {
		InfluxDBSink<TotalTraffic> sink = new InfluxDBSink<TotalTraffic>(influxdbUrl, influxdbUsername,
				influxdbPassword, influxdbDbName) {

			@Override
			public void invoke(TotalTraffic value) {
				try {
					LOG.info("TO INFLUX_1: " + value);
					getInfluxDB().write(org.influxdb.dto.Point.measurement(inputStreamName)
							.time(toSeconds(value.getTime()), TimeUnit.SECONDS)
							.addField("value", value.getValue())
							.build());
				} catch (Throwable e) {
					LOG.error("Error on TotalTraffic INFLUX_1 " + value, e);
				}

			}

		};
		return sink;
	}

	public static RichSinkFunction<TotalTraffic> getSink2(String inputStreamName, String influxdbUrl, String org,
			String token, String bucket) {
		InfluxDB2Sink<TotalTraffic> sink = new InfluxDB2Sink<TotalTraffic>(influxdbUrl, org, token,
				bucket) {

			@Override
			public void invoke(TotalTraffic value) {
//				LOG.info("TO INFLUX_2: "+ value);
//		        try (WriteApi writeApi = getInfluxDB().getWriteApi()) {
//
//		        	com.influxdb.client.write.Point point = com.influxdb.client.write.Point.measurement(inputStreamName)
//		                    .addField("value", value.f1)
//		                    .addTag("class", TotalTrafficReaderToInflux.class.getSimpleName())
//		                    .time(value.f0.getTime(), WritePrecision.MS);
//
//		            writeApi.writePoint(point);
//		        }	

				try {
					LOG.info("TO INFLUX_2: " + value);
					WriteApi writeApi = getInfluxDB().getWriteApi();
					com.influxdb.client.write.Point point = com.influxdb.client.write.Point.measurement(inputStreamName)
							.addField("value", value.getValue())
							.addTag("class", TotalTrafficReaderToInflux.class.getSimpleName())
							.time(toSeconds(value.getTime()), WritePrecision.S);

					writeApi.writePoint(point);
				} catch (Throwable e) {
					LOG.error("Error on TotalTraffic INFLUX_2 " + value, e);
				}
			}

		};
		return sink;
	}

	// TODO TESTARE SU DELL SDP

	@SuppressWarnings("unchecked")
	private FlinkPravegaReader<TotalTraffic> getSourceFunction(PravegaConfig pravegaConfig,
			String inputStreamName) {


		// create the Pravega source to read a stream of text
		FlinkPravegaReader<TotalTraffic> source = FlinkPravegaReader.builder().withPravegaConfig(pravegaConfig)
				.forStream(inputStreamName)
				.withDeserializationSchema(new JsonDeserializationSchema(TotalTraffic.class)).build();
		return source;
	}

	public static void main(String[] args) throws Exception {
		LOG.info("Main...");
		AppConfiguration appConfiguration = new AppConfiguration(args);
		TotalTrafficReaderToInflux reader = new TotalTrafficReaderToInflux(appConfiguration);
		reader.run();
	}

}