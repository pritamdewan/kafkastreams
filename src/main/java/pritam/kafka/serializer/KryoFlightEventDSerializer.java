package pritam.kafka.serializer;

import java.io.Closeable;
import java.util.Map;


import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.pritam.stream.model.BaseEvent;




public class KryoFlightEventDSerializer implements Closeable, AutoCloseable, Serializer<BaseEvent>, Deserializer<BaseEvent> {
    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
         //   kryo.addDefaultSerializer(FlightEvent.class, new KryoInternalSerializer());
            return kryo;
        };
    };

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, BaseEvent fe) {
        ByteBufferOutput output = new ByteBufferOutput(100);
        kryos.get().writeObject(output, fe);
        return output.toBytes();
    }

    @Override
    public BaseEvent deserialize(String s, byte[] bytes) {
        try {
            return kryos.get().readObject(new ByteBufferInput(bytes), BaseEvent.class);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes",e);
        }
    }

    @Override
    public void close() {

    }
/**
    private static class KryoInternalSerializer extends com.esotericsoftware.kryo.Serializer<FlightEvent> {
        @Override
        public void write(Kryo kryo, Output output, FlightEvent fe) {
            output.writeString(fe.getFlightNumber());
            //output.writeString(fe.getEventData());
           // output.writeLong(sensorReading.getTime(), true);
           // output.writeDouble(sensorReading.getValue());
        }

        @Override
        public FlightEvent read(Kryo kryo, Input input, Class<FlightEvent> aClass) {
            String id = input.readString();
            //Sensor.Type type = Sensor.Type.valueOf(input.readString());
            return null;
            //return new SensorReading(new Sensor(id, type), input.readLong(true), input.readDouble());
        }
    }
    **/
}