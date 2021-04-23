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

import com.dellemc.oe.util.AppConfiguration;

import it.consulthink.oe.ingest.NMAJSONInfiniteWriter;
import junit.framework.Assert;

public class TotalTrafficReaderIntegrationTest {
	private static final Logger LOG = LoggerFactory.getLogger(TotalTrafficReaderIntegrationTest.class);

	@ClassRule
	public static MiniClusterWithClientResource flinkCluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(6).setNumberTaskManagers(2)
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
	public void testRun() throws IOException, InterruptedException {
		LOG.info("Starting TotalTrafficReaderIntegrationTest...");


        String scope = "integration-test";
		String myIp = "213.61.202.114,213.61.202.115,213.61.202.116,213.61.202.117,213.61.202.118,213.61.202.119,213.61.202.120,213.61.202.121,213.61.202.122,213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126";
		String inputStreamName = "nma-input";
		String outputStreamName = "total-traffic";
		String controllerUri = "tcp://host.docker.internal:9090";
		String[] args = {
          	  "--myIps", myIp,
      		  "--scope", scope,
      		  "--input-stream", inputStreamName,
      		  "--output-stream", outputStreamName,
      		  "--controller", controllerUri
          };
  		AppConfiguration appConfiguration = new AppConfiguration( args );  
  		
  		LOG.info("Starting NMAJSONInfiniteWriter...");
  		NMAJSONInfiniteWriter generator = new NMAJSONInfiniteWriter(appConfiguration, 1l);
  		Thread thread = new Thread(generator);
  		thread.start();
  		Thread.sleep(3 * 1000);
 
  		LOG.info("Starting TotalTrafficReader...");
        TotalTrafficReader reader = new TotalTrafficReader(appConfiguration);
        reader.run();
	}

	@BeforeClass
	public static void upDockerCompose() throws IOException, InterruptedException {
		dockerCompose("docker-compose up -d --remove-orphans ");
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
