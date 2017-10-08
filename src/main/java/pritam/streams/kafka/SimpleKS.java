package pritam.streams.kafka;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

public class SimpleKS {
	
	private static Properties getProperties() {
        Properties settings = new Properties();
        // Set a few key parameters
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "Streams");
        // Kafka bootstrap server (broker to talk to); ubuntu is the host name for my VM running Kafka, port 9092 is where the (single) broker listens 
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Apache ZooKeeper instance keeping watch over the Kafka cluster; ubuntu is the host name for my VM running Kafka, port 2181 is where the ZooKeeper listens 
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // default serdes for serialzing and deserializing key and value from and to streams in case no specific Serde is specified
        settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\temp");
        // to work around exception Exception in thread "StreamThread-1" java.lang.IllegalArgumentException: Invalid timestamp -1
        // at org.apache.kafka.clients.producer.ProducerRecord.<init>(ProducerRecord.java:60)
        // see: https://groups.google.com/forum/#!topic/confluent-platform/5oT0GRztPBo
        settings.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return settings;
    }


	public static void main(String args[]){
		
		ArrayList<String> stopWords = new ArrayList<String>();
		stopWords.add("bad");
		stopWords.add("mad");
		stopWords.add("sad");
    	
		
		Properties config = getProperties();

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String,String> rawLines = builder.stream("test");
        
        KStream transform = rawLines.map(new KeyValueMapper<String,String,KeyValue<String,String>>() {
        	
        	@Override
            public KeyValue<String, String> apply(String key, String value) {
        		if(value.isEmpty()){
        			return new KeyValue<>("No Key", "This is start");
        		}else{
            		String k = value.substring(0, 5);
            		String v = value .substring(6).trim();
            		
            		return new KeyValue<>(k, v);
        			
        		}
        		
            }
		});
        
        transform.print();
        
        
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
		
	}

}
