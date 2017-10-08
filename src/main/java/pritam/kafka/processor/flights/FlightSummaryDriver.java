package pritam.kafka.processor.flights;

import java.util.Properties;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;

import com.pritam.stream.model.FlightEvent;
import com.pritam.stream.model.PaxBoardEvent;
import com.pritam.stream.model.PaxCheckInEvent;

import pritam.kafka.serializer.JsonDeserializer;
import pritam.kafka.serializer.JsonSerializer;
import pritam.kafka.serializer.KyroGenrericSerializer;

public class FlightSummaryDriver {

	private static Properties getProperties() {
        Properties props = new Properties();
        //props.put(StreamsConfig.CLIENT_ID_CONFIG, "Sample-Stateful-Processor");
        props.put("group.id", "test-flight-group");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateful_processor_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
      //  props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
      //  props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
		//props.put("metadata.broker.list", "localhost:9092");
		//props.put("bootstrap.servers", "localhost:9092");
		//props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		//properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	//	props.put("value.serializer", pritam.kafka.serializer.KryoFlightEventDSerializer.class.getName());

     
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
	

	
	public static void main(String args[]){
		
		
		 StreamsConfig streamingConfig = new StreamsConfig(getProperties());
		 TopologyBuilder builder = new TopologyBuilder();
		 IntegerDeserializer intDeserializer = new IntegerDeserializer();
		 KyroGenrericSerializer<FlightEvent> kyroflightserial = new KyroGenrericSerializer<FlightEvent>(FlightEvent.class);
		 KyroGenrericSerializer<PaxCheckInEvent> kyropaxserialCHK = new KyroGenrericSerializer<PaxCheckInEvent>(PaxCheckInEvent.class);
		 KyroGenrericSerializer<PaxBoardEvent> kyropaxserialBD = new KyroGenrericSerializer<PaxBoardEvent>(PaxBoardEvent.class);
		 
		 
		 
		 JsonSerializer<FlightSummary> flightSummarySerializer = new JsonSerializer<>();
	     JsonDeserializer<FlightSummary> flightSummaryDeserializer = new JsonDeserializer<>(FlightSummary.class);
	     Serde<FlightSummary> flightSummarySerde = Serdes.serdeFrom(flightSummarySerializer,flightSummaryDeserializer);
	     
	     //flight event source topic
	     builder.addSource("flight-source",intDeserializer, kyroflightserial,"flights-ek501");
	     //pax event source topic
	     builder.addSource("chk-source",intDeserializer, kyropaxserialCHK,"chk-ek501");
	     builder.addSource("brd-source",intDeserializer, kyropaxserialBD,"brd-ek501");
	     
	     //add the event processors
		 builder.addProcessor("flight-summary", FlightSummaryProcessor::new, "flight-source","chk-source","brd-source");
		 builder.addProcessor("missing-pax", BoardingSummaryProcessor::new, "flight-summary");
		 
		 //add the persistent storage 
		 builder.addStateStore(Stores.create("flight-movement").withStringKeys().withValues(flightSummarySerde).
				 inMemory().maxEntries(1000).build(), "flight-summary");
		 
		 
		 System.out.println("Starting FlightSummaryStatefulProcessor Example");
	     KafkaStreams streaming = new KafkaStreams(builder, streamingConfig);
	     streaming.start();
		
		
		
	}
	
}
