package pritam.kafka.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.pritam.stream.model.BaseEvent;

public class KyroGenrericSerializer<T> implements Serializer<T>,Deserializer<T> {
	
	private Class<T> deserializedClass;

	private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			// kryo.addDefaultSerializer(FlightEvent.class, new
			// KryoInternalSerializer());
			return kryo;
		};
	};

	public KyroGenrericSerializer(Class<T> deserializedClass){
		this.deserializedClass = deserializedClass;
	}
	
	@Override

	public void configure(Map<String, ?> map, boolean b) {
        if(deserializedClass == null) {
            deserializedClass = (Class<T>) map.get("serializedClass");
        }

	}

	public byte[] serialize(String topic, T t) {
		ByteBufferOutput output = new ByteBufferOutput(100);
		kryos.get().writeObject(output, t);
		return output.toBytes();

	}
	
    public T deserialize(String s, byte[] bytes) {
    	  try {
              return kryos.get().readObject(new ByteBufferInput(bytes),deserializedClass);
          }
          catch(Exception e) {
              throw new IllegalArgumentException("Error reading bytes",e);
          }
    }	

	@Override
	public void close() {

	}
}
