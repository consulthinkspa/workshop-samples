package it.consulthink.oe.readers;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
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
import it.consulthink.oe.model.NMAJSONData;

/*
 * At a high level, DistinctIPReader reads from a Pravega stream, and prints
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
public class DistinctIPReaderToInflux extends AbstractApp {

	// Logger initialization
	private static final Logger LOG = LoggerFactory.getLogger(DistinctIPReaderToInflux.class);

	// The application reads data from specified Pravega stream and once every 10
	// seconds
	// prints the distinct words and counts from the previous 10 seconds.
	public DistinctIPReaderToInflux(AppConfiguration appConfiguration) {
		super(appConfiguration);
	}

	public void run() {
		LOG.info("Starting NMA DistinctIPReaderToInflux...");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		AppConfiguration.StreamConfig inputStreamConfig = appConfiguration.getInputStreamConfig();
		;
		String inputStreamName = inputStreamConfig.getStream().getStreamName();
		createStream(inputStreamConfig);
		LOG.info("============== input stream  =============== " + inputStreamName);

		// Create EventStreamClientFactory
		PravegaConfig pravegaConfig = appConfiguration.getPravegaConfig();
		LOG.info("============== Praevega  =============== " + pravegaConfig);

		SourceFunction<Tuple3<Date, Integer,Integer>> sourceFunction = getSourceFunction(pravegaConfig, inputStreamName);
		LOG.info("==============  SourceFunction  =============== " + sourceFunction);

		DataStream<Tuple3<Date, Integer,Integer>> source = env.addSource(sourceFunction).name("InputSource");
		KeyedStream<Tuple3<Date, Integer,Integer>, Tuple> dataStream = source.keyBy(0);
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

		RichSinkFunction<Tuple3<Date, Integer,Integer>> sink = null;
		
		
		
		if (influxdbVersion.equals("1")) {
			sink = getSink1(inputStreamName, influxdbUrl, influxdbUsername, influxdbPassword, influxdbDbName);
		}else {
			sink = getSink2(inputStreamName, influxdbUrl, org, token, bucket);
		}

		dataStream.addSink(sink).name("InfluxDistinctIPStream");

		// create another output sink to print to stdout for verification

		LOG.info("==============  ProcessSink - PRINTED  ===============");

		// execute within the Flink environment
		try {
			env.execute("DistinctIPReader");
		} catch (Exception e) {
			LOG.error("Error executing DistinctIPReader...");
		} finally {
			LOG.info("Ending NMA DistinctIPReader...");
		}

	}

	public static RichSinkFunction<Tuple3<Date, Integer,Integer>> getSink1(String inputStreamName, String influxdbUrl,
			String influxdbUsername, String influxdbPassword, String influxdbDbName) {
		InfluxDBSink<Tuple3<Date, Integer,Integer>> sink = new InfluxDBSink<Tuple3<Date, Integer,Integer>>(influxdbUrl, influxdbUsername,
				influxdbPassword, influxdbDbName) {

			@Override
			public void invoke(Tuple3<Date, Integer,Integer> value) {
				try {
					getInfluxDB()
							.write(org.influxdb.dto.Point.measurement(inputStreamName)
									.time(toSeconds(value.f0), TimeUnit.SECONDS)
									.addField("value", value.f1 + value.f2)
									.addField("internal", value.f1)
									.addField("external", value.f2)
									.build());
				} catch (Exception e) {
					LOG.error("Error on DistinctIP " + value, e);
				}

			}

		};
		return sink;
	}
	
	public static RichSinkFunction<Tuple3<Date, Integer,Integer>> getSink2(String inputStreamName, String influxdbUrl,
			String org, String token, String bucket) {
		InfluxDB2Sink<Tuple3<Date, Integer,Integer>> sink = new InfluxDB2Sink<Tuple3<Date, Integer,Integer>>(influxdbUrl, org, token, bucket) {

			@Override
			public void invoke(Tuple3<Date, Integer,Integer> value) {
		        try (WriteApi writeApi = getInfluxDB().getWriteApi()) {

		        	com.influxdb.client.write.Point point = com.influxdb.client.write.Point.measurement(inputStreamName)
							.addField("value", value.f1 + value.f2)
							.addField("internal", value.f1)
							.addField("external", value.f2)
		                    .addTag("class", DistinctIPReaderToInflux.class.getSimpleName())
		                    .time(toSeconds(value.f0), WritePrecision.S);

		            writeApi.writePoint(point);
		        }					
			}

		};
		return sink;
	}	


	@SuppressWarnings("unchecked")
	private FlinkPravegaReader<Tuple3<Date, Integer,Integer>> getSourceFunction(PravegaConfig pravegaConfig,
			String inputStreamName) {
		// create the Pravega source to read a stream of text
		FlinkPravegaReader<Tuple3<Date, Integer,Integer>> source = FlinkPravegaReader.builder().withPravegaConfig(pravegaConfig)
				.forStream(inputStreamName).withDeserializationSchema(new JsonDeserializationSchema(Tuple3.class))
				.build();
		return source;
	}

	public static void main(String[] args) throws Exception {
		LOG.info("Starting DistinctIPReaderToInflux...");
		AppConfiguration appConfiguration = new AppConfiguration(args);
		DistinctIPReaderToInflux reader = new DistinctIPReaderToInflux(appConfiguration);
		reader.run();
	}

}
