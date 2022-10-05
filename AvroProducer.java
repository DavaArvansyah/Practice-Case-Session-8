package kafka.producer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import javax.sql.rowset.serial.SerialException;
import java.util.Map;
import java.util.Properties;

public class AvroProducer {
    public static void produce(Map<String, String> configMap, String topic, String key, GenericRecord avroRecord, String schema_url){
        if (configMap.containsKey("bootstrap.servers") && configMap.containsKey("client.id")) {
            Properties properties = new Properties();
            configMap.forEach(properties::setProperty);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schema_url);
            KafkaProducer producer = new KafkaProducer(properties);


//            String userSchema = schema_path;
//            Schema.Parser parser = new Schema.Parser();
//            Schema schema = parser.parse(userSchema);
//            GenericRecord avroRecord = new GenericData.Record(schema);
//            avroRecord.put("key", " ");
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, key, avroRecord);

            try {
                producer.send(record);
            } catch (SerializationException e) {
                e.printStackTrace();
            } finally {
                producer.flush();
                producer.close();
            }
      //      System.out.print(record);
        //    System.out.print(avroRecord);

        }
    }
}