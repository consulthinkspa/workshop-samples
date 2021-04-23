package it.consulthink.oe.readers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;

public class TotalTrafficReaderToInfluxTest {
	private static final Logger LOG = LoggerFactory.getLogger(TotalTrafficReaderToInfluxTest.class);
	
	static String influxdbUrl = "http://host.docker.internal:8086";
	static String influxdbUsername = "admin";
	static String influxdbPassword = "password";
	static String influxdbDbName = "demo";
	
	static InfluxDB influxDB;
	
	
	private static String OS = null;
	@ClassRule
	public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(6).setNumberTaskManagers(2)
					.build());

	@Test
	public void testGetSink() throws Exception {

		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		List<Tuple2<Date, Long>> iterable = Arrays.asList(Tuple2.of(new Date(), 1l), Tuple2.of(new Date(), 2l),
				Tuple2.of(new Date(), 3l), Tuple2.of(new Date(), 4l));



		RichSinkFunction<Tuple2<Date, Long>> sink = TotalTrafficReaderToInflux.getSink("TotalTrafficReaderToInfluxTest",
				influxdbUrl, influxdbUsername, influxdbPassword, influxdbDbName);

		DataStreamSink<Tuple2<Date, Long>> addSink = senv.fromCollection(iterable).keyBy(0).addSink(sink);

		senv.execute();
	}

	public static String getOsName() {
		if (OS == null) {
			OS = System.getProperty("os.name");
		}
		return OS;
	}

	public static boolean isWindows() {
		return getOsName().startsWith("Windows");
	}

	@BeforeClass
	public static void setUp() throws IOException, InterruptedException {
		dockerCompose("docker-compose --file docker-compose-influx.yml up -d ");
		Thread.sleep(5 * 1000);

		
		if (influxdbUsername == null || influxdbUsername.isEmpty()) {
			influxDB = InfluxDBFactory.connect(influxdbUrl);
		} else {
			influxDB = InfluxDBFactory.connect(influxdbUrl, influxdbUsername, influxdbPassword);
		}
		// influxDB = InfluxDBFactory.connect("http://
		// String influxdbDbName = "demo";
//		influxDB.query(new Query("CREATE DATABASE " + influxdbDbName));
		influxDB.setDatabase(influxdbDbName);
//		influxDB.query(new Query("DROP SERIES FROM /.*/"));
	}

//	@AfterClass
//	public static void downDockerCompose() throws IOException, InterruptedException {
//		dockerCompose("docker-compose stop ");
//	}

	private static void dockerCompose(String execCompose) throws IOException, InterruptedException {
		Path path = FileSystems.getDefault().getPath("docker-compose.yml").toAbsolutePath();
		File composeConfig = path.toFile();
		Assert.assertTrue(composeConfig.exists() && composeConfig.isFile());

		String[] args;
		;
		if (isWindows()) {
			args = new String[] { "wsl", "--cd", path.getParent().toAbsolutePath().toString(), "sh", "-c",
					execCompose };
		} else {
			args = new String[] { "sh", "-c",
					"cd " + path.getParent().toAbsolutePath().toString() + " && " + execCompose };
		}
		String command = String.join(" ", args);

		Process process = Runtime.getRuntime().exec(args);
		process.waitFor();
		if (process.exitValue() != 0) {
			InputStream stream = process.getErrorStream();
			int c = 0;
			StringBuffer sb = new StringBuffer();
			while ((c = stream.read()) != -1) {
				sb.append((char) c);
			}
			LOG.error(execCompose + System.lineSeparator() + " ## START DOCKER ERR ##" + System.lineSeparator() + sb
					+ System.lineSeparator() + " ##  END DOCKER ERR  ##");
		} else {
			InputStream stream = process.getInputStream();
			int c = 0;
			StringBuffer sb = new StringBuffer();
			while ((c = stream.read()) != -1) {
				sb.append((char) c);
			}
			LOG.info(execCompose + System.lineSeparator() + " ## START DOCKER OUT ##" + System.lineSeparator() + sb
					+ System.lineSeparator() + " ##  END DOCKER OUT  ##");
		}

	}

}
