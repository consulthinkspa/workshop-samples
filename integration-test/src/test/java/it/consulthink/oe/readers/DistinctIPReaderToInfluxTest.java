package it.consulthink.oe.readers;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.influxdb.InfluxDB;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.influxdb.client.InfluxDBClient;

import junit.framework.Assert;

public class DistinctIPReaderToInfluxTest {
	private static final Logger LOG = LoggerFactory.getLogger(DistinctIPReaderToInfluxTest.class);
	public static final String DOCKER_COMPOSE_YML = "docker-compose-influx.yml";
	
	static String influxdb1Url = "http://host.docker.internal:8086";
	static String influxdbUsername = "admin";
	static String influxdbPassword = "password";
	static String influxdbDbName = "nma";
	
	static InfluxDB influxDB1;
	
	static String influxdb2Url = "http://host.docker.internal:8186";
	static String org = "it.consulthink";
	static String token = "D1ARPWX51_G5fP93DI9TYB13cvP_E0qN4yzFDktafhpzXul2-ItLLqKben2qyzMnjkibyAd-ag4A14Iifrq95A==";
	static String bucket = "nma";
	
	static InfluxDBClient influxDB2;
	
	
	private static String OS = null;
	@ClassRule
	public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(6).setNumberTaskManagers(2)
					.build());
	@Test
	public void testGetSink1() throws Exception {

		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		List<Tuple3<Date, Integer, Integer>> iterable = Arrays.asList(Tuple3.of(new Date(), 1,1), Tuple3.of(new Date(), 2,2),
				Tuple3.of(new Date(), 3,3), Tuple3.of(new Date(), 4,4));



		String inputStreamName = this.getClass()+".testGetSink1";
		RichSinkFunction<Tuple3<Date, Integer, Integer>> sink = DistinctIPReaderToInflux.getSink1(inputStreamName, influxdb1Url,
				influxdbUsername, influxdbPassword, influxdbDbName);

		DataStreamSink<Tuple3<Date, Integer, Integer>> addSink = senv.fromCollection(iterable).keyBy(0).addSink(sink);

		senv.execute();
	}
	
	@Test
	public void testGetSink2() throws Exception {

		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		List<Tuple3<Date, Integer, Integer>> iterable = Arrays.asList(Tuple3.of(new Date(), 1,1), Tuple3.of(new Date(), 2,2),
				Tuple3.of(new Date(), 3,3), Tuple3.of(new Date(), 4,4));



		String inputStreamName = this.getClass()+".testGetSink2";
		RichSinkFunction<Tuple3<Date, Integer, Integer>> sink = DistinctIPReaderToInflux.getSink2(inputStreamName, influxdb2Url,
				org, token, bucket);

		DataStreamSink<Tuple3<Date, Integer, Integer>> addSink = senv.fromCollection(iterable).keyBy(0).addSink(sink);

		senv.execute();
	}
	
	@Test
	public void testGetSink1Infinite() throws Exception {

		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		java.util.Random r = new java.util.Random(System.currentTimeMillis());
		
		List<Tuple3<Date, Integer, Integer>> iterable = Lists.newArrayList(Stream.generate(new Supplier<Tuple3<Date, Integer, Integer>>() {

			@Override
			public Tuple3<Date, Integer, Integer> get() {

				try {
					Thread.sleep(500);
				} catch (Throwable e) {}

				return Tuple3.of(new Date(), r.nextInt(1024),r.nextInt(1024));
			}
		
			
		})
		.limit(120)
		.iterator());


		String inputStreamName = this.getClass()+".testGetSink1Infinite";
		RichSinkFunction<Tuple3<Date, Integer, Integer>> sink = DistinctIPReaderToInflux.getSink1(inputStreamName, influxdb1Url,
				influxdbUsername, influxdbPassword, influxdbDbName);

//		FromIteratorFunction<Tuple3<Date, Integer, Integer>> source = new FromIteratorFunction<Tuple3<Date, Integer, Integer>>(iterator);
		DataStreamSink<Tuple3<Date, Integer, Integer>> sinked = senv.fromCollection(iterable).keyBy(0).addSink(sink);

		senv.execute();
	}
	
	@Test
	public void testGetSink2Infinite() throws Exception {

		StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
		senv.setParallelism(3);
		senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		java.util.Random r = new java.util.Random(System.currentTimeMillis());
		
		List<Tuple3<Date, Integer, Integer>> iterable = Lists.newArrayList(Stream.generate(new Supplier<Tuple3<Date, Integer, Integer>>() {

			@Override
			public Tuple3<Date, Integer, Integer> get() {
				return Tuple3.of(new Date(), r.nextInt(1024),r.nextInt(1024));
			}
		
			
		})
		.limit(120)
		.iterator());


		String inputStreamName = this.getClass()+".testGetSink2Infinite";
		RichSinkFunction<Tuple3<Date, Integer, Integer>> sink = DistinctIPReaderToInflux.getSink2(inputStreamName, influxdb2Url,
				org, token, bucket);

//		FromIteratorFunction<Tuple3<Date, Integer, Integer>> source = new FromIteratorFunction<Tuple3<Date, Integer, Integer>>(iterator);
		DataStreamSink<Tuple3<Date, Integer, Integer>> sinked = senv.fromCollection(iterable).keyBy(0).addSink(sink);

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
		dockerCompose("docker-compose --file "+DOCKER_COMPOSE_YML+" up -d ");
		Thread.sleep(5 * 1000);

	}

//	@AfterClass
//	public static void downDockerCompose() throws IOException, InterruptedException {
//		dockerCompose("docker-compose stop ");
//	}

	private static void dockerCompose(String execCompose) throws IOException, InterruptedException {
		Path path = FileSystems.getDefault().getPath(DOCKER_COMPOSE_YML).toAbsolutePath();
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
