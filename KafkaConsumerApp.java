import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class KafkaConsumerApp {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer myConsumer = new KafkaConsumer(props);
    }
}
