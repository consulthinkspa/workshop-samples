package it.consulthink.oe.readers;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
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
import it.consulthink.oe.model.Anomaly;
import it.consulthink.oe.model.NMAJSONData;

/*
 * At a high level, AnomalyReader reads from a Pravega stream, and prints
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
public class AnomalyControlReaderToInflux extends AbstractApp {

	// Logger initialization
	private static final Logger LOG = LoggerFactory.getLogger(AnomalyControlReaderToInflux.class);

	// The application reads data from specified Pravega stream and once every 10
	// seconds
	// prints the distinct words and counts from the previous 10 seconds.
	public AnomalyControlReaderToInflux(AppConfiguration appConfiguration) {
		super(appConfiguration);
	}

	public static BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> getTimestampAndWatermarkAssigner() {
		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = new BoundedOutOfOrdernessTimestampExtractor<NMAJSONData>(
				Time.seconds(1)) {

			@Override
			public long extractTimestamp(NMAJSONData element) {
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

		SourceFunction<NMAJSONData> sourceFunction = getSourceFunction(pravegaConfig, inputStreamName);

		DataStream<NMAJSONData> source = env.addSource(sourceFunction).name("Pravega." + inputStreamName);

		BoundedOutOfOrdernessTimestampExtractor<NMAJSONData> timestampAndWatermarkAssigner = getTimestampAndWatermarkAssigner();


		String influxdbUrl = appConfiguration.getInfluxdbUrl();

		String influxdbVersion = appConfiguration.getInfluxdbVersion();

		String influxdbUsername = appConfiguration.getInfluxdbUsername();
		String influxdbPassword = appConfiguration.getInfluxdbPassword();
		String influxdbDbName = appConfiguration.getInfluxdbDb();

		String org = appConfiguration.getInfluxdbOrg();
		String token = appConfiguration.getInfluxdbToken();
		String bucket = appConfiguration.getInfluxdbBucket();

		RichSinkFunction<NMAJSONData> sink = null;

		if (influxdbVersion.equals("1")) {
			sink = getSink1(inputStreamName, influxdbUrl, influxdbUsername, influxdbPassword, influxdbDbName);
		} else {
			sink = getSink2(inputStreamName, influxdbUrl, org, token, bucket);
		}
		
		DataStreamSink<NMAJSONData> dataStream = source
				.assignTimestampsAndWatermarks(timestampAndWatermarkAssigner)
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

	public static RichSinkFunction<NMAJSONData> getSink1(String inputStreamName, String influxdbUrl,
			String influxdbUsername, String influxdbPassword, String influxdbDbName) {
		InfluxDBSink<NMAJSONData> sink = new InfluxDBSink<NMAJSONData>(influxdbUrl, influxdbUsername,
				influxdbPassword, influxdbDbName) {

			@Override
			public void invoke(NMAJSONData value) {
				try {
					LOG.info("TO INFLUX_1 "+inputStreamName+" : " + value);
					getInfluxDB().write(org.influxdb.dto.Point.measurement(inputStreamName)
							.time(toSeconds(value.getTime()), TimeUnit.SECONDS)
							.addField("value", 1)
							.addField("Src_ip", value.getSrc_ip())
							.addField("Sport", value.getSport())
							.addField("Dst_ip", value.getDst_ip())
							.addField("Dport", value.getDport())
							.addField("Pkts", value.getPkts())
							.addField("Pkts", value.getBytesin() + value.getBytesout())
							.addField("SessionState", value.getSessionState())
							.addField("SessionHash", value.getSessionHash())
							.build());
				} catch (Throwable e) {
					LOG.error("Error on TO INFLUX_1 "+inputStreamName+" : " + value, e);
				}

			}



		};
		return sink;
	}

	public static RichSinkFunction<NMAJSONData> getSink2(String inputStreamName, String influxdbUrl, String org,
			String token, String bucket) {
		InfluxDB2Sink<NMAJSONData> sink = new InfluxDB2Sink<NMAJSONData>(influxdbUrl, org, token,
				bucket) {

			@Override
			public void invoke(NMAJSONData value) {
//				LOG.info("TO INFLUX_2: "+ value);
//		        try (WriteApi writeApi = getInfluxDB().getWriteApi()) {
//
//		        	com.influxdb.client.write.Point point = com.influxdb.client.write.Point.measurement(inputStreamName)
//		                    .addField("value", value.f1)
//		                    .addTag("class", AnomalyReaderToInflux.class.getSimpleName())
//		                    .time(value.f0.getTime(), WritePrecision.MS);
//
//		            writeApi.writePoint(point);
//		        }	

				try {
					LOG.info("TO INFLUX_2 "+inputStreamName+" : " + value);
					WriteApi writeApi = getInfluxDB().getWriteApi();
					com.influxdb.client.write.Point point = com.influxdb.client.write.Point.measurement(inputStreamName)
							.addField("value", 1)
							.addField("Src_ip", value.getSrc_ip())
							.addField("Sport", value.getSport())
							.addField("Dst_ip", value.getDst_ip())
							.addField("Dport", value.getDport())
							.addField("Pkts", value.getPkts())
							.addField("Pkts", value.getBytesin() + value.getBytesout())
							.addField("SessionState", value.getSessionState())
							.addField("SessionHash", value.getSessionHash())
							.addTag("class", AnomalyControlReaderToInflux.class.getSimpleName())
							.time(toSeconds(value.getTime()), WritePrecision.S);

					writeApi.writePoint(point);
				} catch (Throwable e) {
					LOG.error("Error on TO INFLUX_2 "+inputStreamName+" : " + value, e);
				}
			}

		};
		return sink;
	}

	// TODO TESTARE SU DELL SDP

    @SuppressWarnings("unchecked")
    private FlinkPravegaReader<NMAJSONData> getSourceFunction(PravegaConfig pravegaConfig, String inputStreamName) {
        // create the Pravega source to read a stream of text
        FlinkPravegaReader<NMAJSONData> source = FlinkPravegaReader.builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(inputStreamName)
                .withDeserializationSchema(new JsonDeserializationSchema(NMAJSONData.class))
                .build();
        return source;
    }

	public static void main(String[] args) throws Exception {
		LOG.info("Main...");
		AppConfiguration appConfiguration = new AppConfiguration(args);
		AnomalyControlReaderToInflux reader = new AnomalyControlReaderToInflux(appConfiguration);
		reader.run();
	}

}