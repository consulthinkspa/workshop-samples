package it.consulthink.oe.db;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InfluxDBSink<IN> extends RichSinkFunction<IN> {
	private static transient ThreadLocal<InfluxDB> influxDB_TL = new ThreadLocal<InfluxDB>();
	private static transient final Logger LOG = LoggerFactory.getLogger(InfluxDBSink.class);
	String influxdbUrl = "";
	String influxdbUsername = "";
	String influxdbPassword = "";
	String influxdbDbName = "";

	public InfluxDBSink() {
		super();
		this.influxDB_TL = new ThreadLocal<InfluxDB>();
	}

	public InfluxDBSink(String influxdbUrl, String influxdbUsername, String influxdbPassword, String influxdbDbName) {
		this();
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
	public synchronized void  open(Configuration config) {
		LOG.info("Open "+config);
		if (influxDB_TL == null) {
			influxDB_TL = new ThreadLocal<InfluxDB>();
		}
		if (influxdbUsername == null || influxdbUsername.isEmpty()) {
			influxDB_TL.set(InfluxDBFactory.connect(influxdbUrl));
		} else {
			influxDB_TL.set(InfluxDBFactory.connect(influxdbUrl, influxdbUsername, influxdbPassword));
		}
		influxDB_TL.get().setDatabase(influxdbDbName);
	}

	@Override
	public void close() throws Exception {
		LOG.info("Close ");
		if (getInfluxDB() != null) {
			getInfluxDB().close();
		}
	}
	
	public synchronized InfluxDB getInfluxDB() {
		if (influxDB_TL == null) {
			influxDB_TL = new ThreadLocal<InfluxDB>();
		}
		if (influxDB_TL.get() == null) {
			this.open(null);
		}
		return influxDB_TL.get();
	}
}