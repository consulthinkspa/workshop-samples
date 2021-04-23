package com.dellemc.oe.util;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Assert;

public class AppConfigurationTest {
	private static final Logger LOG = LoggerFactory.getLogger(AppConfigurationTest.class);
	@Test
	public void testAppConfiguration() {
		
        String scope = "test-workshop-samples";
		String myIp = "213.61.202.114";
		String inputStreamName = "test-input";
		String outputStreamName = "test-output";
		String controllerUri = "tcp://host.docker.internal:9090";
		String[] args = {
          	  "--myIps", myIp,
      		  "--scope", scope,
      		  "--input-stream", inputStreamName,
      		  "--output-stream", outputStreamName,
      		  "--controller", controllerUri,
      		  "--minNumSegments", "3",
      		  "--input-targetRate", "150",
      		  "--input-scaleFactor", "3",
      		  "--input-minNumSegments", "4"
      		  
          };
  		AppConfiguration appConfiguration = new AppConfiguration( args );
  		
  		Assert.assertEquals(1, appConfiguration.getMyIps().size());
  		Assert.assertEquals(scope, appConfiguration.getInputStreamConfig().getStream().getScope());
  		Assert.assertEquals(scope, appConfiguration.getOutputStreamConfig().getStream().getScope());
  		Assert.assertEquals(inputStreamName, appConfiguration.getInputStreamConfig().getStream().getStreamName());
  		Assert.assertEquals(150, appConfiguration.getInputStreamConfig().getTargetRate());
  		Assert.assertEquals(3, appConfiguration.getInputStreamConfig().getScaleFactor());
  		Assert.assertEquals(4, appConfiguration.getInputStreamConfig().getMinNumSegments());
  		
  		
  		Assert.assertEquals(outputStreamName, appConfiguration.getOutputStreamConfig().getStream().getStreamName());
  		Assert.assertEquals(100000, appConfiguration.getOutputStreamConfig().getTargetRate());
  		Assert.assertEquals(2, appConfiguration.getOutputStreamConfig().getScaleFactor());
  		Assert.assertEquals(3, appConfiguration.getOutputStreamConfig().getMinNumSegments());  		
  		
		Assert.assertEquals(controllerUri, appConfiguration.getPravegaConfig().getClientConfig().getControllerURI().toString());
  		
  		
	}

}
