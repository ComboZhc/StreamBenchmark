package chao.cmu.capstone;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Generate tweets from Twitter Sample Streaming API given rate in time series
 */
public class ThroughputLoadGenerator {
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 4) {
            System.out.println("Usage: <brokers> <topic> <time> <thrupts>");
            System.out.println("<thrupts> could be a single number <thrupt>");
            System.out.println("<thrupts> could also be a time series of number <thrupt1>,<time1>,<thrupt2>,<time2>...");
            System.out.println("<thrupt> could be either a number or +/- number");
            System.out.println("<time> unit is in second");
            System.exit(-1);
        }
        String brokers = args[0];
        String topic = args[1];
        long duration = Long.valueOf(args[2]) * 1000;
        String[] symbols = args[3].split(",");
        if (symbols.length == 1) {
            symbols = new String[] {symbols[0], "1"};
        }
        if (symbols.length % 2 != 0) {
            System.out.println("<thrupts> expects a single number or an even number of numbers");
            System.exit(-1);
        }
        int lastThrupt = 0;
        int[] thrupts = new int[symbols.length / 2];
        long[] times = new long[symbols.length / 2];
        for (int i = 0; i < symbols.length / 2; i++) {
            String amp = symbols[i * 2];
            int currThrupt;
            switch (amp.charAt(0)) {
                case '+':
                    currThrupt = lastThrupt + Integer.valueOf(amp.substring(1));
                    break;
                case '-':
                    currThrupt = lastThrupt - Integer.valueOf(amp.substring(1));
                    break;
                default:
                    currThrupt = Integer.valueOf(amp);
                    break;
            }
            thrupts[i] = currThrupt;
            lastThrupt = currThrupt;
            times[i] = Long.valueOf(symbols[i * 2 + 1]) * 1000;
        }
        System.out.println(String.format("Brokers = %s", brokers));
        System.out.println(String.format("Topic = %s", topic));
        System.out.println(String.format("Duration = %d seconds", duration / 1000));
        System.out.println(String.format("Pattern = %s", args[3]));

        KafkaProducer<String, String> producer = Utils.getKafkaProducer(brokers, AmplifyLoadGenerator.class.getName());
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        Client client = Utils.getTwitterClient(queue);
        client.connect();
        WindowStats stats = new WindowStats(duration);
        final TimedValue timedValue = new TimedValue(thrupts, times, stats, "Target (rec/s)");
        final Sender sender = new Sender(producer, topic, stats);
        stats.addListener(new WindowStats.Listener() {
            @Override
            public void onTick() {
                sender.setCount(sender.getCount() + timedValue.getValue());
            }
        });

        String msg = queue.take();
        stats.start();
        timedValue.start();
        sender.start();

        while (!stats.stopped()) {
            if (Utils.isTweet(msg)) {
                stats.onSource();
                sender.setMsg(msg);
            }
            msg = queue.take();
        }
        stats.printTotal();
        sender.stop();
        timedValue.stop();
        client.stop();
        producer.close();
    }

    public static class Sender extends TimerTask {
        private Timer timer = new Timer();
        private KafkaProducer<String, String> producer;
        private String topic;
        private String msg;
        private WindowStats stats;
        private int count;

        public Sender(KafkaProducer<String, String> producer, String topic, WindowStats stats) {
            this.producer = producer;
            this.topic = topic;
            this.stats = stats;
            this.msg = null;
            this.count = 0;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public void start() {
            timer.scheduleAtFixedRate(this, 0, 10);
        }

        public void stop() { timer.cancel(); }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        @Override
        public void run() {
            if (msg != null) {
                int currCount = (count + 49) / 50;
                for (int i = 0; i < currCount; i++) {
                    producer.send(new ProducerRecord<String, String>(topic, msg), stats.onSend(msg.length()));
                    count--;
                }
            }
            stats.putParameter("Queued (recs)", count);
        }
    }
}
