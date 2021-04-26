package it.consulthink.oe.db;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;

public abstract class InfluxDB2Sink<IN> extends RichSinkFunction<IN> {
	InfluxDBClient  influxDB = null;
	String influxdbUrl = "";
	String org = "";
	String token = "";
	String bucket = "";

	public InfluxDB2Sink() {
	}

	public InfluxDB2Sink(String influxdbUrl, String org, String token, String bucket) {
		this.influxdbUrl = influxdbUrl;
		this.org = org;
		this.token = token;
		this.bucket = bucket;
	}

	public abstract void invoke(IN value);     

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
	public void open(Configuration config) {
		influxDB = InfluxDBClientFactory.create(influxdbUrl, token.toCharArray(), org, bucket);
	}

	@Override
	public void close() throws Exception {
		if (getInfluxDB() != null) {
			getInfluxDB().close();
		}
	}
	
	public InfluxDBClient getInfluxDB() {
		return influxDB;
	}
}