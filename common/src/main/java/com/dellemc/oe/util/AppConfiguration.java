/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package com.dellemc.oe.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.connectors.flink.PravegaConfig;

// All parameters will come from environment variables. This makes it easy
// to configure on Docker, Mesos, Kubernetes, etc.
public class AppConfiguration {
    private static Logger log = LoggerFactory.getLogger(AppConfiguration.class);

    private final PravegaConfig pravegaConfig;
    private final StreamConfig inputStreamConfig;
    private final StreamConfig outputStreamConfig;
    private int parallelism;
    private long checkpointInterval;
    private boolean enableCheckpoint;
    private boolean enableOperatorChaining;
    private boolean enableRebalance;
    private String routingKey;
    private String dataFile;
    private String message;
    private String csvDir;
    private Set<String> myIps = new HashSet<String>();
    private Set<String> inputDuplicators = new HashSet<String>();
    
    private String influxdbUrl =  "http://host.docker.internal:8086";
    
    private String influxdbVersion = "1";
    
    private String influxdbUsername = "admin";
    private String influxdbPassword = "password";
    private String influxdbDb = "nma";

	private String influxdbOrg = "it.consulthink";
    private String influxdbToken = "D1ARPWX51_G5fP93DI9TYB13cvP_E0qN4yzFDktafhpzXul2-ItLLqKben2qyzMnjkibyAd-ag4A14Iifrq95A==";
	private String influxdbBucket = "nma";    
	
    public static boolean createStream(AppConfiguration appConfiguration, AppConfiguration.StreamConfig streamConfig) {
        boolean result = false;
        try(StreamManager streamManager = StreamManager.create(appConfiguration.getPravegaConfig().getClientConfig())) {
        	if (!streamManager.checkScopeExists(streamConfig.getStream().getScope())) {
        		streamManager.createScope(streamConfig.getStream().getScope());
        	}
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byDataRate(streamConfig.getTargetRate(), streamConfig.getScaleFactor(), streamConfig.getMinNumSegments()))
                    .build();
        	if (streamManager.checkStreamExists(streamConfig.getStream().getScope(), streamConfig.getStream().getStreamName())){
//                result = streamManager.updateStream(streamConfig.getStream().getScope(), streamConfig.getStream().getStreamName(), streamConfiguration);        		
        	}else {
                result = streamManager.createStream(streamConfig.getStream().getScope(), streamConfig.getStream().getStreamName(), streamConfiguration);
        	}

            log.info("Creating Pravega Stream: " +result+ " "+ streamConfig.getStream().getStreamName() +" "+ streamConfig.getStream().getScope() + streamConfiguration);
        }
        return result;
    }

	public AppConfiguration(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        log.info("Parameter Tool: {}", params.toMap());

        pravegaConfig = PravegaConfig.fromParams(params).withDefaultScope("examples"); // TODO: make configurable
        inputStreamConfig = new StreamConfig(pravegaConfig, "input-", params);
        outputStreamConfig = new StreamConfig(pravegaConfig, "output-", params);
        parallelism = params.getInt("parallelism", 1);
        checkpointInterval = params.getLong("checkpointInterval", 10000);     // milliseconds
        enableCheckpoint = params.getBoolean("enableCheckpoint", true);
        enableOperatorChaining = params.getBoolean("enableOperatorChaining", true);
        enableRebalance = params.getBoolean("rebalance", false);
        routingKey = params.get("routing-key", "default");
        dataFile = params.get("dataFile", "earthquakes1970-2014.csv");
        message = params.get("message", "hello world");
        csvDir = params.get("csvDir", "/mnt/flink");
        
        myIps = new HashSet<String>(Arrays.asList(params.get("myIps",
                "213.61.202.114,213.61.202.115,213.61.202.116," +
                        "213.61.202.117,213.61.202.118,213.61.202.119," +
                        "213.61.202.120,213.61.202.121,213.61.202.122," +
                        "213.61.202.123,213.61.202.124,213.61.202.125,213.61.202.126").split(",")));
        
        inputDuplicators = new HashSet<String>(Arrays.asList(params.get("inputDuplicators",
                "datacontrol,total-traffic,anomaly,traffic-by-direction,synin,sessions,distinctips").split(",")));
        
        influxdbUrl =  params.get("influxdbUrl", "http://host.docker.internal:8086");
        influxdbVersion =  params.get("influxdbVersion", "1");
        
        influxdbUsername =  params.get("influxdbUsername", "admin");
        influxdbPassword =  params.get("influxdbPassword", "password");
        influxdbDb =  params.get("influxdbDb", "nma");
        
        influxdbOrg = params.get("influxdbOrg", "it.consulthink");
        influxdbToken = params.get("influxdbToken", "D1ARPWX51_G5fP93DI9TYB13cvP_E0qN4yzFDktafhpzXul2-ItLLqKben2qyzMnjkibyAd-ag4A14Iifrq95A==");
        influxdbBucket = params.get("influxdbBucket", "nma");           
    }

    public String getCsvDir() {return csvDir;}

    public Set<String> getInputDuplicators() {
		return inputDuplicators;
	}
    
    public Set<String> getMyIps() {
		return myIps;
	}
    
    
    public String getInfluxdbUrl() {
		return influxdbUrl;
	}
    

	public String getInfluxdbVersion() {
		return influxdbVersion;
	}

	public String getInfluxdbUsername() {
		return influxdbUsername;
	}

	public String getInfluxdbPassword() {
		return influxdbPassword;
	}

	public String getInfluxdbDb() {
		return influxdbDb;
	}

	public String getInfluxdbOrg() {
		return influxdbOrg;
	}

	public String getInfluxdbToken() {
		return influxdbToken;
	}

	public String getInfluxdbBucket() {
		return influxdbBucket;
	}

	public String getMessage() {
        return message;
    }

    public String getDataFile() {
        return dataFile;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public PravegaConfig getPravegaConfig() {
        return pravegaConfig;
    }

    public StreamConfig getInputStreamConfig() {
        return inputStreamConfig;
    }

    public StreamConfig getOutputStreamConfig() {
        return outputStreamConfig;
    }

    public int getParallelism() {
        return parallelism;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public boolean isEnableCheckpoint() {
        return enableCheckpoint;
    }

    public boolean isEnableOperatorChaining() {
        return enableOperatorChaining;
    }

    public boolean isEnableRebalance() {
        return enableRebalance;
    }

    public static class StreamConfig implements Serializable, Cloneable{
        /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		protected Stream stream;
        protected int targetRate;
        protected int scaleFactor;
        protected int minNumSegments;

        public StreamConfig(PravegaConfig pravegaConfig, String argPrefix, ParameterTool params) {
            this.stream = pravegaConfig.resolve(params.get(argPrefix + "stream", "default"));
            this.targetRate = params.getInt(argPrefix + "targetRate", 100000);  // Data rate in KB/sec
            this.scaleFactor = params.getInt(argPrefix + "scaleFactor", 2);
            this.minNumSegments = params.getInt(argPrefix + "minNumSegments", 3);
        }
        
        private StreamConfig(Stream stream, int targetRate, int scaleFactor, int minNumSegments) {
        	this.stream = stream;
        	this.targetRate = targetRate;
        	this.scaleFactor = scaleFactor;
        	this.minNumSegments = minNumSegments;
        }
        

        public Stream getStream() {
            return stream;
        }
        
        public void updateStream(String scope, String name) {
        	this.stream = Stream.of(scope, name);
        }

        public int getTargetRate() {
            return targetRate;
        }

        public int getScaleFactor() {
            return scaleFactor;
        }

        public int getMinNumSegments() {
            return minNumSegments;
        }


		@Override
		public String toString() {
			return "StreamConfig [stream=" + stream + ", targetRate=" + targetRate + ", scaleFactor=" + scaleFactor
					+ ", minNumSegments=" + minNumSegments + "]";
		}


		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + minNumSegments;
			result = prime * result + scaleFactor;
			result = prime * result + ((stream == null) ? 0 : stream.hashCode());
			result = prime * result + targetRate;
			return result;
		}


		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StreamConfig other = (StreamConfig) obj;
			if (minNumSegments != other.minNumSegments)
				return false;
			if (scaleFactor != other.scaleFactor)
				return false;
			if (stream == null) {
				if (other.stream != null)
					return false;
			} else if (!stream.equals(other.stream))
				return false;
			if (targetRate != other.targetRate)
				return false;
			return true;
		}
		
		@Override
		public StreamConfig clone() throws CloneNotSupportedException {
			StreamConfig result = new StreamConfig(Stream.of(this.stream.getScope(), this.stream.getStreamName()), this.targetRate, this.scaleFactor, this.minNumSegments);
			return result;
		}
        
        
    }

	@Override
	public String toString() {
		return "AppConfiguration [pravegaConfig=" + pravegaConfig + ", inputStreamConfig=" + inputStreamConfig
				+ ", outputStreamConfig=" + outputStreamConfig + ", parallelism=" + parallelism
				+ ", checkpointInterval=" + checkpointInterval + ", enableCheckpoint=" + enableCheckpoint
				+ ", enableOperatorChaining=" + enableOperatorChaining + ", enableRebalance=" + enableRebalance
				+ ", routingKey=" + routingKey + ", dataFile=" + dataFile + ", message=" + message + ", csvDir="
				+ csvDir + ", myIps=" + myIps + ", influxdbUrl=" + influxdbUrl + ", influxdbVersion=" + influxdbVersion
				+ ", influxdbUsername=" + influxdbUsername + ", influxdbPassword=" + influxdbPassword + ", influxdbDb="
				+ influxdbDb + ", influxdbOrg=" + influxdbOrg + ", influxdbToken=" + influxdbToken + ", influxdbBucket="
				+ influxdbBucket + "]";
	}
}
