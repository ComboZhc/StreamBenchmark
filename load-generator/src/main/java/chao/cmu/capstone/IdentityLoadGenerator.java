package chao.cmu.capstone;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Generate identity tweets from Twitter Sample Streaming API
 */
public class IdentityLoadGenerator {
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.out.println("Usage: <brokers> <topic> <time>");
            System.exit(-1);
        }
        String brokers = args[0];
        String topic = args[1];
        long duration = Long.valueOf(args[2]) * 1000;
        System.out.println(String.format("Brokers = %s", brokers));
        System.out.println(String.format("Topic = %s", topic));
        System.out.println(String.format("Duration = %d seconds", duration / 1000));

        KafkaProducer<String, String> producer = Utils.getKafkaProducer(brokers, IdentityLoadGenerator.class.getName());
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        Client client = Utils.getTwitterClient(queue);
        client.connect();

        WindowStats stats = new WindowStats(duration);

        String msg = queue.take();
        stats.start();
        while (!stats.stopped()) {
            if (Utils.isTweet(msg)) {
                stats.onSource();
                producer.send(new ProducerRecord<String, String>(topic, msg), stats.onSend(msg.length()));
            }
            msg = queue.take();
        }
        stats.printTotal();
        client.stop();
        producer.close();
    }
}
