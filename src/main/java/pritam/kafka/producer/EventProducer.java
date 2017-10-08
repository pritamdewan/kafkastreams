package pritam.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.pritam.stream.model.FlightEvent;
import com.pritam.stream.model.PaxBoardEvent;
import com.pritam.stream.model.PaxCheckInEvent;



public class EventProducer {

	public static Properties getProperties() {
		Properties properties = new Properties();
		properties.put("acks", "all");
		properties.put("retries", 0);
		properties.put("batch.size", 16384);
		properties.put("linger.ms", 1);
		properties.put("buffer.memory", 33554432);
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		//properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", pritam.kafka.serializer.KryoFlightEventDSerializer.class.getName());
		return properties;
	}

	public static void doFlightOpen(String flightNumber, Integer flightNo) {

		FlightEvent data = null;
		data = new FlightEvent(flightNumber, "Open");
		Producer<Integer, FlightEvent> producer = getFEProducer();
		producer.send(new ProducerRecord<Integer, FlightEvent>("flights-ek501", flightNo, data));
		producer.close();
		System.out.println("EventProducer <<<<<<<<<<<<<<<<<<<<Flight Opened for >>>>>>>>>>>>>>>>>>>>>>>" + flightNumber);
	}

	

	public static void doFlightClose(String flightNumber, Integer flightNo) {
		FlightEvent data = null;
		data = new FlightEvent(flightNumber, "Close");
		Producer<Integer, FlightEvent> producer = getFEProducer();
		producer.send(new ProducerRecord<Integer, FlightEvent>("flights-ek501", flightNo, data));
		producer.close();
		System.out.println("EventProducer <<<<<<<<<<<<<<<<<<<<Flight Closed for >>>>>>>>>>>>>>>>>>>>>>>" + flightNumber);
	}

	
	public static void doCheckIn(String flightNumber) {
		Producer<Integer, PaxCheckInEvent> producer = getCHKProducer();

		for (int i = 0; i <3; i++) {
			try {
				PaxCheckInEvent ps = new PaxCheckInEvent(flightNumber, ("pnr000" + i), ("pax100" + i),true);
				producer.send(new ProducerRecord<Integer,PaxCheckInEvent>("chk-ek501", i, ps));
				Thread.sleep(3000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
			}

		}
		producer.close();
		System.out.println("EventProducer <<<<<<<<<<<<<<<<<<<<Check in Closed for >>>>>>>>>>>>>>>>>>>>>>>" + flightNumber);

	}

	public static void doBoarding(String flightNumber) {
		Producer<Integer, PaxBoardEvent> producer = getBDProducer();

		for (int i = 0; i < 2; i++) {
			try {
				PaxBoardEvent ps = new PaxBoardEvent(flightNumber, ("pnr000" + i), ("pax100" + i),true);

				producer.send(new ProducerRecord<Integer,PaxBoardEvent>("brd-ek501", i,  ps));
//				JSONDao.getInstance(PaxBoardEvent.class).update(ps);
				Thread.sleep(3000);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
			}

		}
		producer.close();
		System.out.println("EventProducer <<<<<<<<<<<<<<<<<<<<Boarding Closed for >>>>>>>>>>>>>>>>>>>>>>>" + flightNumber);
	}

	private static Producer<Integer, PaxBoardEvent> getBDProducer() {
		final Producer<Integer, PaxBoardEvent> producer;
		producer = new KafkaProducer<>(getProperties());
		return producer;
	}

	private static Producer<Integer, FlightEvent> getFEProducer() {
		final Producer<Integer, FlightEvent> producer;
		producer = new KafkaProducer<>(getProperties());
		return producer;
	}

	private static Producer<Integer, PaxCheckInEvent> getCHKProducer() {
		final Producer<Integer, PaxCheckInEvent> producer;
		producer = new KafkaProducer<>(getProperties());
		return producer;
	}

	public static void runProducer() {

		// Open the Flight
		doFlightOpen("EK501", 501);
		// Check in Pax 
		doCheckIn("EK501");
		//Boarding complete
		doBoarding("EK501");
		//flight close indicator
		doFlightClose("EK501", 501);
	}

	public static void main(String args[]) {
		EventProducer.runProducer();
	}

}
