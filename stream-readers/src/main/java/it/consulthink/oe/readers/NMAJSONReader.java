
package it.consulthink.oe.readers;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dellemc.oe.serialization.JsonDeserializationSchema;
import com.dellemc.oe.util.AbstractApp;
import com.dellemc.oe.util.AppConfiguration;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import it.consulthink.oe.model.NMAJSONData;

import java.util.Date;

/*
 *  This flink application demonstrates the JSON Data reading
 */
public class NMAJSONReader extends AbstractApp {

    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(NMAJSONReader.class);
    private static final int READER_TIMEOUT_MS = 3000;

    public NMAJSONReader(AppConfiguration appConfiguration){
        super(appConfiguration);
    }

    public void run() {
        try {
            // Create client config
            PravegaConfig pravegaConfig =  appConfiguration.getPravegaConfig();
            LOG.info("==============  NMA pravegaConfig  =============== " + pravegaConfig);
            // create the Pravega input stream (if necessary)
            createStream(appConfiguration.getInputStreamConfig());
            Stream stream = appConfiguration.getInputStreamConfig().getStream();

            StreamExecutionEnvironment env = initializeFlinkStreaming();
            // create the Pravega source to read a stream of text
            FlinkPravegaReader<Tuple2<Date,Long>> flinkPravegaReader = FlinkPravegaReader.builder()
                    .withPravegaConfig(pravegaConfig)
                    .forStream(stream)
                    .withDeserializationSchema(new JsonDeserializationSchema(Tuple2.class))
                    .build();

            DataStream<Tuple2<Date,Long>> events = env
                    .addSource(flinkPravegaReader)
                    .name("events");

            // create an output sink to print to stdout for verification
            events.printToErr();

            // execute within the Flink environment
            env.execute("NMA JSON Reader");

            LOG.info("########## JSON READER END #############");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        LOG.info("########## NMA READER START #############");
        AppConfiguration appConfiguration = new AppConfiguration(args);
        NMAJSONReader jsonReader = new NMAJSONReader(appConfiguration);
        jsonReader.run();
    }

}
