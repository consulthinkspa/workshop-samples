package it.consulthink.oe.db;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

public abstract class InfluxDBSink<IN> extends RichSinkFunction<IN> {
	InfluxDB influxDB = null;
	String influxdbUrl = "";
	String influxdbUsername = "";
	String influxdbPassword = "";
	String influxdbDbName = "";

	public InfluxDBSink() {
	}

	public InfluxDBSink(String influxdbUrl, String influxdbUsername, String influxdbPassword, String influxdbDbName) {
		this.influxdbUrl = influxdbUrl;
		this.influxdbUsername = influxdbUsername;
		this.influxdbPassword = influxdbPassword;
		this.influxdbDbName = influxdbDbName;
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
		if (influxdbUsername == null || influxdbUsername.isEmpty()) {
			influxDB = InfluxDBFactory.connect(influxdbUrl);
		} else {
			influxDB = InfluxDBFactory.connect(influxdbUrl, influxdbUsername, influxdbPassword);
		}
//        influxDB.query(new Query("CREATE DATABASE " + influxdbDbName));
		influxDB.setDatabase(influxdbDbName);
//        influxDB.query(new Query("DROP SERIES FROM /.*/"));
	}

	@Override
	public void close() throws Exception {
		if (getInfluxDB() != null) {
			getInfluxDB().close();
		}
	}
	
	public InfluxDB getInfluxDB() {
		return influxDB;
	}
}