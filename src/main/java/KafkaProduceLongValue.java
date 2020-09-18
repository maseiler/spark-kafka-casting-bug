import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProduceLongValue {
    private final Logger LOG = LoggerFactory.getLogger(KafkaProduceLongValue.class.getName());

    public KafkaProduceLongValue() {
        KafkaProducer<Long, byte[]> producer = createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close, "thread-shutdown-producer"));

        byte[] byteArray = new byte[0];
        for (int i = 1; i <= 200; i++) {
            producer.send(new ProducerRecord<>("spark-long-test", (long) i, byteArray), LogError());
            sleep();
        }
        producer.close();
    }

    public static void main(String[] args) throws InterruptedException {
        new KafkaProduceLongValue();
    }

    private KafkaProducer<Long, byte[]> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "random-country-producer");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // exactly once

        return new KafkaProducer<>(properties);
    }

    private Callback LogError() {
        return new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    LOG.error("Producer error: ", exception);
                }
            }
        };
    }

    void sleep() {
        try {
            Thread.sleep(800);
        } catch (InterruptedException e) {
            LOG.error("Unhandled Exception: ", e);
        }
    }
}
