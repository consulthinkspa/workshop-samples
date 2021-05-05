package it.consulthink.oe.db;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FakeDBSink<IN> extends RichSinkFunction<IN> {
	private static transient final Logger LOG = LoggerFactory.getLogger(FakeDBSink.class);


	public FakeDBSink() {
		super();
	}


    @Override
    public void invoke(IN value) {
    	LOG.info("Sink.invoke :: "+value);
        System.err.println(value);
    }

	@Override
	public synchronized void  open(Configuration config) {
		LOG.info("Open "+config);

	}

	@Override
	public void close() throws Exception {
		LOG.info("Close ");
	}
	
}