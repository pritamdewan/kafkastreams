package pritam.streams.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import pritam.kafka.producer.EventProducer;

public class FlightStreamConsumer {
	
	private static Properties getProperties() {
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "Streams");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\temp");
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return settings;
    }

	public static void runConsumer(){
		
//		final String SOURCE_TOPIC = "test";
		//final String STORE_NAME = "someStateStore";
		 
		// topology
		KStreamBuilder flight_builder = new KStreamBuilder();

		KStream<Integer, String> flight_stream = flight_builder.stream("flights-ek501");	
		KTable<Integer,Long> flight_table_stream = flight_stream.countByKey("EK501");
		flight_table_stream.print();

		KafkaStreams streams = new KafkaStreams(flight_builder, getProperties());
        streams.start();

	}
	public static void main(String args[]){
		
		FlightStreamConsumer.runConsumer();
	}

}
