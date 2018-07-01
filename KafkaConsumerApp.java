import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerApp {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092, localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test");

        KafkaConsumer myConsumer = new KafkaConsumer(props);

        ArrayList<String> topics = new ArrayList<String>();
        topics.add("my-topic");
        topics.add("my-other-topic");

        myConsumer.subscribe(topics);// set the topic subscription

        try{
            while (true){
                ConsumerRecords<String, String> records = myConsumer.poll(10);// poll loop, continuously polling the brokers for data, primary function of Kafka Consumer
                for (ConsumerRecord<String, String> record : records){
                    System.out.println(String.format
                            ("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        } finally {
            myConsumer.close();
        }
    }
}
