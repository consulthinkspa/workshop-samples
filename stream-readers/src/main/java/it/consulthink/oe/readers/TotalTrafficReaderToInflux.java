package it.consulthink.oe.readers;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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

	public void run() {
		LOG.info("Starting NMA TotalTrafficReaderToInflux...");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		AppConfiguration.StreamConfig inputStreamConfig = appConfiguration.getInputStreamConfig();
		;
		String inputStreamName = inputStreamConfig.getStream().getStreamName();
		createStream(inputStreamConfig);
		LOG.info("============== input stream  =============== " + inputStreamName);

		// Create EventStreamClientFactory
		PravegaConfig pravegaConfig = appConfiguration.getPravegaConfig();
		LOG.info("============== Praevega  =============== " + pravegaConfig);

		SourceFunction<Tuple2<Date, Long>> sourceFunction = getSourceFunction(pravegaConfig, inputStreamName);
		LOG.info("==============  SourceFunction  =============== " + sourceFunction);

		DataStream<Tuple2<Date, Long>> source = env.addSource(sourceFunction).name("InputSource");
		KeyedStream<Tuple2<Date, Long>, Tuple> dataStream = source.keyBy(0);
		LOG.info("==============  Source  =============== " + source);

		LOG.info("==============  ProcessSource - PRINTED  ===============");

		String influxdbUrl = appConfiguration.getInfluxdbUrl();
		
		String influxdbVersion = appConfiguration.getInfluxdbVersion();

		String influxdbUsername = appConfiguration.getInfluxdbUsername();
		String influxdbPassword = appConfiguration.getInfluxdbPassword();
		String influxdbDbName = appConfiguration.getInfluxdbDb();
		

		String org = appConfiguration.getInfluxdbOrg();
		String token = appConfiguration.getInfluxdbToken();
		String bucket = appConfiguration.getInfluxdbBucket();

		RichSinkFunction<Tuple2<Date, Long>> sink = null;
		
		
		
		if (influxdbVersion.equals("1")) {
			sink = getSink1(inputStreamName, influxdbUrl, influxdbUsername, influxdbPassword, influxdbDbName);
		}else {
			sink = getSink2(inputStreamName, influxdbUrl, org, token, bucket);
		}

		dataStream.addSink(sink).name("InfluxTotalTrafficStream");

		// create another output sink to print to stdout for verification

		LOG.info("==============  ProcessSink - PRINTED  ===============");

		// execute within the Flink environment
		try {
			env.execute("TotalTrafficReader");
		} catch (Exception e) {
			LOG.error("Error executing TotalTrafficReader...");
		} finally {
			LOG.info("Ending NMA TotalTrafficReader...");
		}

	}

	public static RichSinkFunction<Tuple2<Date, Long>> getSink1(String inputStreamName, String influxdbUrl,
			String influxdbUsername, String influxdbPassword, String influxdbDbName) {
		InfluxDBSink<Tuple2<Date, Long>> sink = new InfluxDBSink<Tuple2<Date, Long>>(influxdbUrl, influxdbUsername,
				influxdbPassword, influxdbDbName) {

			@Override
			public void invoke(Tuple2<Date, Long> value) {
				try {
					LOG.info("TO INFLUX_1: "+ value);
					getInfluxDB()
							.write(org.influxdb.dto.Point.measurement(inputStreamName)
									.time(value.f0.getTime(), TimeUnit.MILLISECONDS)
									.addField("value", value.f1)
									.build());
				} catch (Throwable e) {
					LOG.error("Error on TotalTraffic INFLUX_1 " + value, e);
				}

			}

		};
		return sink;
	}
	
	public static RichSinkFunction<Tuple2<Date, Long>> getSink2(String inputStreamName, String influxdbUrl,
			String org, String token, String bucket) {
		InfluxDB2Sink<Tuple2<Date, Long>> sink = new InfluxDB2Sink<Tuple2<Date, Long>>(influxdbUrl, org, token, bucket) {

			@Override
			public void invoke(Tuple2<Date, Long> value) {
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
				LOG.info("TO INFLUX_2: "+ value);
		        	WriteApi writeApi = getInfluxDB().getWriteApi();
		        	com.influxdb.client.write.Point point = com.influxdb.client.write.Point.measurement(inputStreamName)
		                    .addField("value", value.f1)
		                    .addTag("class", TotalTrafficReaderToInflux.class.getSimpleName())
		                    .time(value.f0.getTime(), WritePrecision.MS);

		            writeApi.writePoint(point);
				} catch (Throwable e) {
					LOG.error("Error on TotalTraffic INFLUX_2 " + value, e);
				}
			}
			


		};
		return sink;
	}	


	@SuppressWarnings("unchecked")
	private FlinkPravegaReader<Tuple2<Date, Long>> getSourceFunction(PravegaConfig pravegaConfig,
			String inputStreamName) {
		// create the Pravega source to read a stream of text
		FlinkPravegaReader<Tuple2<Date, Long>> source = FlinkPravegaReader.builder().withPravegaConfig(pravegaConfig)
				.forStream(inputStreamName).withDeserializationSchema(new JsonDeserializationSchema(Tuple2.class))
				.build();
		return source;
	}

	public static void main(String[] args) throws Exception {
		LOG.info("Starting TotalTrafficReaderToInflux...");
		AppConfiguration appConfiguration = new AppConfiguration(args);
		TotalTrafficReaderToInflux reader = new TotalTrafficReaderToInflux(appConfiguration);
		reader.run();
	}

}
