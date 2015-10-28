package chao.cmu.capstone;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class IdentityLoadGenerator {
    private final static Logger logger = LoggerFactory.getLogger(IdentityLoadGenerator.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: <brokers> <topic>");
            System.exit(-1);
        }
        String brokers = args[0];
        String topic = args[1];

        KafkaProducer<String, String> producer = Utils.getKafkaProducer(brokers, IdentityLoadGenerator.class.getName());
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        Client client = Utils.getTwitterClient(queue);
        client.connect();

        boolean started = false;
        WindowStats stats = new WindowStats(1000);

        while (!client.isDone()) {
            try {
                String msg = queue.take();
                if (!started) {
                    started = true;
                    stats.start();
                }
                JSONObject json = new JSONObject(msg);
                if (!json.isNull("text")) {
                    producer.send(new ProducerRecord<String, String>(topic, msg), stats.next(msg.length()));
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted", e);
            } catch (JSONException e) {
                logger.error("JSON Parse Error", e);
            }
        }
    }
}
