package it.consulthink.oe.db;

import java.time.temporal.ChronoUnit;
import java.util.Date;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;

public abstract class InfluxDB2Sink<IN> extends RichSinkFunction<IN> {
	private static transient ThreadLocal<InfluxDBClient> influxDB_TL = new ThreadLocal<InfluxDBClient>();
	private static transient final Logger LOG = LoggerFactory.getLogger(InfluxDB2Sink.class);
	String influxdbUrl = "";
	String org = "";
	String token = "";
	String bucket = "";

	public InfluxDB2Sink() {
		super();
		influxDB_TL = new ThreadLocal<InfluxDBClient>();
	}

	public InfluxDB2Sink(String influxdbUrl, String org, String token, String bucket) {
		this.influxdbUrl = influxdbUrl;
		this.org = org;
		this.token = token;
		this.bucket = bucket;
	}

	public abstract void invoke(IN value);     
	
	public static Long toSeconds(Date time) {
		if (time == null)
			return null;
		return time.toInstant().truncatedTo(ChronoUnit.SECONDS).getEpochSecond();
	}

//    @Override
//    public void invoke(T value) {
//        try {
//            System.out.println("value: " + value);
//            influxDB.write(Point.measurement(value.getSensorid())
//                    .time(value.getTimestamp(), TimeUnit.MILLISECONDS)
//                    .addField("DIFFERENCE", value.getDifference())
//                    .addField("TREND", value.getTrend())
//                    .addField("AVERAGE", value.getAverage())
//                    .build());
//        } catch(Exception e) {
//            System.out.println("Failed!");
//            e.printStackTrace();
//        }
//    }

	@Override
	public synchronized void open(Configuration config) {
		LOG.info("Open "+config);
		if (influxDB_TL == null) {
			influxDB_TL = new ThreadLocal<InfluxDBClient>();
		}
		if (influxDB_TL.get() == null) {
			influxDB_TL.set(InfluxDBClientFactory.create(influxdbUrl, token.toCharArray(), org, bucket));
		} 
	}

	@Override
	public void close() throws Exception {
		LOG.info("Close ");
		if (getInfluxDB() != null) {
			getInfluxDB().close();
		}
	}
	
	public synchronized InfluxDBClient getInfluxDB() {
		if (influxDB_TL == null || influxDB_TL.get() == null) {
			this.open(null);
		}		
		return influxDB_TL.get();
	}
}