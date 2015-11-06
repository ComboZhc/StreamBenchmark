package chao.cmu.capstone;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStats {
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            System.out.println("Usage: <time>");
            System.exit(-1);
        }
        long duration = Long.valueOf(args[0]) * 1000;
        System.out.println(String.format("Duration = %d seconds", duration / 1000));

        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        Client client = Utils.getTwitterClient(queue);
        client.connect();

        WindowStats stats = new WindowStats(duration);

        String msg = queue.take();
        stats.start();
        while (!stats.stopped()) {
            if (Utils.isTweet(msg)) {
                stats.onSource();
            }
            msg = queue.take();
        }
        stats.printTotal();
        client.stop();
    }
}
