package it.consulthink.oe.flink.packetcount;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.flink.wordcount.WordCountReader;
import com.dellemc.oe.ingest.EventWriter;
import com.dellemc.oe.util.AppConfiguration;

import it.consulthink.oe.ingest.NMAJSONInfiniteWriter;
import it.consulthink.oe.readers.TotalTrafficReaderToInflux;
import junit.framework.Assert;

public class WordCountIntegrationTest {
	public static final String DOCKER_COMPOSE_YML = "docker-compose.yml";

	private static final Logger LOG = LoggerFactory.getLogger(WordCountIntegrationTest.class);
	private final static int flinkParallelism = 2;

	@ClassRule
	public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(flinkParallelism).setNumberTaskManagers(flinkParallelism)
					.build());	

	private static String OS = null;

	public static String getOsName() {
		if (OS == null) {
			OS = System.getProperty("os.name");
		}
		return OS;
	}

	public static boolean isWindows() {
		return getOsName().startsWith("Windows");
	}
	
	@Test
	public void testRun1() throws IOException, InterruptedException {
		LOG.info("Starting testRun1...");


        String scope = "integration-test";
		String myIp = "213.61.202.114,213.61.202.115,213.61.202.116,213.61.202.117,213.61.202.118,213.61.202.119,213.61.202.120,213.61.202.121,213.61.202.122,213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126";
		String inputStreamName = "event-input";
		String outputStreamName = "wordcount";
		String controllerUri = "tcp://host.docker.internal:9090";
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
  		
  		LOG.info("Starting Thread EventWriter...");
  		EventWriter ingestor = new EventWriter(new AppConfiguration( argsIngestor ));
  		ingestor.run();
        
	}
	
	@Test
	public void testRun2() throws IOException, InterruptedException {
		LOG.info("Starting testRun2...");


        String scope = "integration-test";
		String myIp = "213.61.202.114,213.61.202.115,213.61.202.116,213.61.202.117,213.61.202.118,213.61.202.119,213.61.202.120,213.61.202.121,213.61.202.122,213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126";
		String inputStreamName = "event-input";
		String outputStreamName = "wordcount";
		String controllerUri = "tcp://host.docker.internal:9090";
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
  		
  		LOG.info("Starting Thread EventWriter...");
  		EventWriter ingestor = new EventWriter(new AppConfiguration( argsIngestor ));
  		Thread threadIngestor = new Thread(ingestor);
  		threadIngestor.start();
  		Thread.sleep(3 * 1000);
  		
		String[] argsProcessor = {
	          	  "--myIps", myIp,
	      		  "--scope", scope,
	      		  "--parallelism", String.valueOf(flinkParallelism),	      		  
	      		  "--input-stream", inputStreamName,
	      		  "--output-stream", outputStreamName,
	      		  "--controller", controllerUri,
	      		  "--input-targetRate", "150",
	      		  "--input-scaleFactor", "1",
	      		  "--input-minNumSegments", "1",
	      		  "--output-targetRate", "150",
	      		  "--output-scaleFactor", "1",
	      		  "--output-minNumSegments", "1"      		  
	          };
 
  		LOG.info("Starting Thread WordCountReader...");
  		WordCountReader processor = new WordCountReader(new AppConfiguration( argsProcessor ));
  		processor.run();
//  		Thread threadProcessor= new Thread(processor);
//  		threadProcessor.start();
//  		Thread.sleep(10 * 1000);
        
	}

	@Test
	public void testRun3() throws IOException, InterruptedException {
		LOG.info("Starting testRun3...");


        String scope = "integration-test";
		String myIp = "213.61.202.114,213.61.202.115,213.61.202.116,213.61.202.117,213.61.202.118,213.61.202.119,213.61.202.120,213.61.202.121,213.61.202.122,213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126";
		String inputStreamName = "event-input";
		String outputStreamName = "wordcount";
		String controllerUri = "tcp://host.docker.internal:9090";
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
  		
  		LOG.info("Starting Thread EventWriter...");
  		EventWriter ingestor = new EventWriter(new AppConfiguration( argsIngestor ));
  		Thread threadIngestor = new Thread(ingestor);
  		threadIngestor.start();
  		Thread.sleep(3 * 1000);
  		
		String[] argsProcessor = {
	          	  "--myIps", myIp,
	      		  "--scope", scope,
	      		  "--parallelism", String.valueOf(flinkParallelism),	      		  
	      		  "--input-stream", inputStreamName,
	      		  "--output-stream", outputStreamName,
	      		  "--controller", controllerUri,
	      		  "--input-targetRate", "150",
	      		  "--input-scaleFactor", "1",
	      		  "--input-minNumSegments", "1",
	      		  "--output-targetRate", "150",
	      		  "--output-scaleFactor", "1",
	      		  "--output-minNumSegments", "1"      		  
	          };
 
  		LOG.info("Starting Thread WordCountReader...");
  		WordCountReader processor = new WordCountReader(new AppConfiguration( argsProcessor ));
  		Thread threadProcessor= new Thread(processor);
  		threadProcessor.start();
        Thread.sleep(10 * 1000);
        
        
        
	}

	@BeforeClass
	public static void upDockerCompose() throws IOException, InterruptedException {
		dockerCompose("docker-compose up -f "+ DOCKER_COMPOSE_YML + " -d --remove-orphans ");
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

		if (isWindows()) {
			args = new String[] { "wsl", "--cd", path.getParent().toAbsolutePath().toString(), "sh", "-c",
					execCompose };
		} else {
			args = new String[] { "sh", "-c", "cd " + path.getParent().toAbsolutePath().toString()
					+ " && "+ execCompose };
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
			LOG.error(execCompose + System.lineSeparator() + " ## START DOCKER ERR ##" + System.lineSeparator() + sb + System.lineSeparator() + " ##  END DOCKER ERR  ##" );
		} else {
			InputStream stream = process.getInputStream();
			int c = 0;
			StringBuffer sb = new StringBuffer();
			while ((c = stream.read()) != -1) {
				sb.append((char) c);
			}
			LOG.info(execCompose + System.lineSeparator() + " ## START DOCKER OUT ##"  + System.lineSeparator() + sb + System.lineSeparator() + " ##  END DOCKER OUT  ##" );
		}
		
		
	}	

}
