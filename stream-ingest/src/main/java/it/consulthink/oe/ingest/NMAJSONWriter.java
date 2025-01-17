package it.consulthink.oe.ingest;

import com.dellemc.oe.serialization.JsonNodeSerializer;
import com.dellemc.oe.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/**
 * A simple example app that uses a Pravega Writer to write to a given scope and stream.
 */
public class NMAJSONWriter {
    // Logger initialization
    private static final Logger LOG = LoggerFactory.getLogger(NMAJSONWriter.class);

    public NMAJSONWriter() {

    }

    public void run() {

        ObjectNode message = null;
        try {

            URI controllerURI = Parameters.getControllerURI();
            StreamManager streamManager = StreamManager.create(controllerURI);
            String scope = Parameters.getScope();
            String streamName = Parameters.getStreamName();
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.byEventRate(
                            Parameters.getTargetRateEventsPerSec(), Parameters.getScaleFactor(), Parameters.getMinNumSegments()))
                    .build();
            streamManager.createStream(scope, streamName, streamConfig);

            ClientConfig config = ClientConfig
            		.builder()
            		.controllerURI(controllerURI)
                    .credentials(null)
                    .trustStore("")
                    .build();
            
            EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scope, config);
            // Create  Pravega event writer

            EventStreamWriter<JsonNode> writer = clientFactory.createEventWriter(
                    streamName,
                    new JsonNodeSerializer(),
                    EventWriterConfig.builder().build());

            //  Coverst CSV  data to JSON
            String data = DataGenerator.convertCsvToJson(Parameters.getDataFile());
            // Deserialize the JSON message.
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonArray = objectMapper.readTree(data);
            if (jsonArray.isArray()) {
                for (JsonNode node : jsonArray) {
                    message = (ObjectNode) node;
                    LOG.info("@@@@@@@@@@@@@ NMA DATA  @@@@@@@@@@@@@  "+message.toString());
                    final CompletableFuture writeFuture = writer.writeEvent(Parameters.getRoutingKey(), message);
                    writeFuture.get();
                    Thread.sleep(10000);
                }

            }
        } catch (Exception e) {
            LOG.error("@@@@@@@@@@@@@ NMA ERROR  @@@@@@@@@@@@@  " + e.getMessage());
        }
    }
    public static void main(String[] args) {
        // Get the Program parameters
        NMAJSONWriter ew = new NMAJSONWriter();
        ew.run();
    }
}
