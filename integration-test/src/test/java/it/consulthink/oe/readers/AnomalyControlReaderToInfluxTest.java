package it.consulthink.oe.readers;

import com.dellemc.oe.util.AppConfiguration;
import com.google.common.collect.Lists;
import com.influxdb.client.InfluxDBClient;

import it.consulthink.oe.flink.packetcount.TotalTrafficReaderIntegrationTest;
import it.consulthink.oe.ingest.NMAJSONInfiniteWriter;
import it.consulthink.oe.ingest.TotalTrafficInfiniteWriter;
import it.consulthink.oe.model.Traffic;
import junit.framework.Assert;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.influxdb.InfluxDB;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class AnomalyControlReaderToInfluxTest {
	private static final String DOCKER_HOST = "host.docker.internal";
	public static final String DOCKER_COMPOSE_YML = "docker-compose.yml";

	private static final Logger LOG = LoggerFactory.getLogger(TotalTrafficReaderIntegrationTest.class);
	private final static int flinkParallelism = 2;
	
	static String influxdb1Url = "http://host.docker.internal:8086";
	static String influxdbUsername = "admin";
	static String influxdbPassword = "password";
	static String influxdbDbName = "nma";
	
	static InfluxDB influxDB1;
	
	static String influxdb2Url = "http://" + DOCKER_HOST + ":8186";
	static String org = "it.consulthink";
	static String token = "D1ARPWX51_G5fP93DI9TYB13cvP_E0qN4yzFDktafhpzXul2-ItLLqKben2qyzMnjkibyAd-ag4A14Iifrq95A==";
	static String bucket = "nma";
	
	static InfluxDBClient influxDB2;
	
	
	private static String OS = null;
	@ClassRule
	public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(33).setNumberTaskManagers(2)
					.build());
	
	@Test
	public void testRun1() throws IOException, InterruptedException {
		LOG.info("Starting testRun1...");


        String scope = "integration-test";
		String myIp = "213.61.202.114,213.61.202.115,213.61.202.116,213.61.202.117,213.61.202.118,213.61.202.119,213.61.202.120,213.61.202.121,213.61.202.122,213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126";

		String inputStreamName = "nma-input";
		String controllerUri = "tcp://" + DOCKER_HOST + ":9090";
		String[] argsIngestor = {
          	  "--myIps", myIp,
      		  "--scope", scope,
      		  "--parallelism", String.valueOf(flinkParallelism),
      		  "--input-stream", inputStreamName,
      		  "--controller", controllerUri,
      		  "--input-targetRate", "150",
      		  "--input-scaleFactor", "1",
      		  "--input-minNumSegments", "1",
      		  "--output-targetRate", "150",
      		  "--output-scaleFactor", "1",
      		  "--output-minNumSegments", "1"      		  
          };
  		
  		LOG.info("Starting Thread NMAJSONInfiniteWriter...");
  		NMAJSONInfiniteWriter ingestor = new NMAJSONInfiniteWriter(new AppConfiguration( argsIngestor ), 10000l);
  		ingestor.run();
	}
	
	

	@Test
	public void testRun2() throws IOException, InterruptedException {
		LOG.info("Starting testRun2...");


        String scope = "integration-test";
		String myIp = "213.61.202.114,213.61.202.115,213.61.202.116,213.61.202.117,213.61.202.118,213.61.202.119,213.61.202.120,213.61.202.121,213.61.202.122,213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126";

		String inputStreamName = "nma-input";
		String controllerUri = "tcp://" + DOCKER_HOST + ":9090";
		
		
        
		String[] argsWriter = {
				
	          	  "--myIps", myIp,
	          	  "--scope", scope,
	      		  "--parallelism", String.valueOf(flinkParallelism),
	      		  "--input-stream", inputStreamName,
	      		  "--controller", controllerUri,
	      		  "--influxdbUrl", "http://" + DOCKER_HOST + ":8086",
	      		  "--influxdbVersion", "1",
	      		  "--influxdbUsername", "admin",
	      		  "--influxdbPassword", "password",
	      		  "--influxdbDb", "nma",	      		  
	      		  "--input-targetRate", "150",
	      		  "--input-scaleFactor", "1",
	      		  "--input-minNumSegments", "1",
	      		  "--output-targetRate", "150",
	      		  "--output-scaleFactor", "1",
	      		  "--output-minNumSegments", "1"      		  
	          };
        
  		LOG.info("Starting Thread AnomalyControlReaderToInflux...");
  		AnomalyControlReaderToInflux writer = new AnomalyControlReaderToInflux(new AppConfiguration( argsWriter ));
  		writer.run();
  		
        
	}	
	
	@Test
	public void testRun3() throws IOException, InterruptedException {
		LOG.info("Starting testRun3...");


        String scope = "integration-test";
		String myIp = "213.61.202.114,213.61.202.115,213.61.202.116,213.61.202.117,213.61.202.118,213.61.202.119,213.61.202.120,213.61.202.121,213.61.202.122,213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126";

		String inputStreamName = "nma-input";
		String controllerUri = "tcp://" + DOCKER_HOST + ":9090";

  		
		String[] argsWriterAnomaly = {
				
	          	  "--myIps", myIp,
	          	  "--scope", scope,
	      		  "--parallelism", String.valueOf(flinkParallelism),
	      		  "--input-stream", inputStreamName + "-anomaly",
	      		  "--controller", controllerUri,
	      		  "--influxdbUrl", "http://" + DOCKER_HOST + ":8086",
	      		  "--influxdbVersion", "1",
	      		  "--influxdbUsername", "admin",
	      		  "--influxdbPassword", "password",
	      		  "--influxdbDb", "nma",	      		  
	      		  "--input-targetRate", "150",
	      		  "--input-scaleFactor", "1",
	      		  "--input-minNumSegments", "1",
	      		  "--output-targetRate", "150",
	      		  "--output-scaleFactor", "1",
	      		  "--output-minNumSegments", "1"      		  
	          };
  		
		AnomalyControlReaderToInflux writerAnomaly = new AnomalyControlReaderToInflux(new AppConfiguration( argsWriterAnomaly ));
//  		Thread threadWiterAnomaly = new Thread(writerAnomaly);
//  		threadWiterAnomaly.start();
//  		Thread.sleep(3 * 1000);
  		
  			writerAnomaly.run();
        
	}	
	
	@Test
	public void testRun() throws IOException, InterruptedException {
		LOG.info("Starting testRun...");


        String scope = "integration-test";
		String myIp = "213.61.202.114,213.61.202.115,213.61.202.116,213.61.202.117,213.61.202.118,213.61.202.119,213.61.202.120,213.61.202.121,213.61.202.122,213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126";

		String inputStreamName = "nma-input";
		String controllerUri = "tcp://" + DOCKER_HOST + ":9090";
		String[] argsIngestor = {
          	  "--myIps", myIp,
      		  "--scope", scope,
      		  "--parallelism", String.valueOf(flinkParallelism),
      		  "--input-stream", inputStreamName,
      		  "--controller", controllerUri,
      		  "--input-targetRate", "150",
      		  "--input-scaleFactor", "1",
      		  "--input-minNumSegments", "1",
      		  "--output-targetRate", "150",
      		  "--output-scaleFactor", "1",
      		  "--output-minNumSegments", "1"      		  
          };
  		
  		LOG.info("Starting Thread NMAJSONInfiniteWriter...");
  		NMAJSONInfiniteWriter ingestor = new NMAJSONInfiniteWriter(new AppConfiguration( argsIngestor ), 10000l);
  		Thread threadIngestor = new Thread(ingestor);
  		threadIngestor.start();
  		Thread.sleep(3 * 1000);
		
        
		String[] argsWriter = {
				
	          	  "--myIps", myIp,
	          	  "--scope", scope,
	      		  "--parallelism", String.valueOf(flinkParallelism),
	      		  "--input-stream", inputStreamName,
	      		  "--controller", controllerUri,
	      		  "--influxdbUrl", "http://" + DOCKER_HOST + ":8086",
	      		  "--influxdbVersion", "1",
	      		  "--influxdbUsername", "admin",
	      		  "--influxdbPassword", "password",
	      		  "--influxdbDb", "nma",	      		  
	      		  "--input-targetRate", "150",
	      		  "--input-scaleFactor", "1",
	      		  "--input-minNumSegments", "1",
	      		  "--output-targetRate", "150",
	      		  "--output-scaleFactor", "1",
	      		  "--output-minNumSegments", "1"      		  
	          };
        
  		LOG.info("Starting Thread TotalTrafficReaderToInflux...");
  		AnomalyControlReaderToInflux writer = new AnomalyControlReaderToInflux(new AppConfiguration( argsWriter ));
  		Thread threadWiter = new Thread(writer);
  		threadWiter.start();
  		Thread.sleep(3 * 1000);
  		
		String[] argsWriterAnomaly = {
				
	          	  "--myIps", myIp,
	          	  "--scope", scope,
	      		  "--parallelism", String.valueOf(flinkParallelism),
	      		  "--input-stream", inputStreamName + "-anomaly",
	      		  "--controller", controllerUri,
	      		  "--influxdbUrl", "http://" + DOCKER_HOST + ":8086",
	      		  "--influxdbVersion", "1",
	      		  "--influxdbUsername", "admin",
	      		  "--influxdbPassword", "password",
	      		  "--influxdbDb", "nma",	      		  
	      		  "--input-targetRate", "150",
	      		  "--input-scaleFactor", "1",
	      		  "--input-minNumSegments", "1",
	      		  "--output-targetRate", "150",
	      		  "--output-scaleFactor", "1",
	      		  "--output-minNumSegments", "1"      		  
	          };
  		
		AnomalyControlReaderToInflux writerAnomaly = new AnomalyControlReaderToInflux(new AppConfiguration( argsWriterAnomaly ));
//  		Thread threadWiterAnomaly = new Thread(writerAnomaly);
//  		threadWiterAnomaly.start();
//  		Thread.sleep(3 * 1000);
  		
  			writerAnomaly.run();
        
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
