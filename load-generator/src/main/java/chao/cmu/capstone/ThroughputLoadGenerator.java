package chao.cmu.capstone;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ThroughputLoadGenerator {
    private final static Logger logger = LoggerFactory.getLogger(ThroughputLoadGenerator.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: <brokers> <topic> <thrupts>");
            System.out.println("<thrupts> could be a single number <thrupt>");
            System.out.println("<thrupts> could also be a time series of number <thrupt1>,<time1>,<thrupt2>,<time2>...");
            System.out.println("<thrupt> could be either a number or +/- number");
            System.out.println("<time> unit is in second");
            System.exit(-1);
        }
        String brokers = args[0];
        String topic = args[1];
        String[] symbols = args[2].split(",");
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

        KafkaProducer<String, String> producer = Utils.getKafkaProducer(brokers, AmplifyLoadGenerator.class.getName());
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        Client client = Utils.getTwitterClient(queue);
        client.connect();
        final TimedValue timedValue = new TimedValue(thrupts, times);
        final WindowStats stats = new WindowStats();
        final WindowSender sender = new WindowSender(producer, topic, stats);
        stats.addListener(new WindowListener() {
            @Override
            public void onWindow() {
                sender.setCount(sender.getCount() + timedValue.getValue());
                logger.info("target = {} count = {}", timedValue.getValue(), sender.getCount());
            }
        });

        boolean started = false;
        while (!client.isDone()) {
            try {
                String msg = queue.take();
                if (!started) {
                    started = true;
                    stats.start();
                    timedValue.start();
                    sender.start();
                }
                if (Utils.isTweet(msg)) {
                    sender.setMsg(msg);
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted", e);
            }
        }
    }

    public static class WindowSender extends TimerTask {
        private Timer timer = new Timer();
        private KafkaProducer<String, String> producer;
        private String topic;
        private String msg;
        private WindowStats stats;
        private int count;

        public WindowSender(KafkaProducer<String, String> producer, String topic, WindowStats stats) {
            this.producer = producer;
            this.topic = topic;
            this.stats = stats;
            this.msg = null;
            this.count = 0;
        }

        public void start() {
            timer.scheduleAtFixedRate(this, 0, 10);
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }

        @Override
        public void run() {
            if (msg != null) {
                int currCount = (count + 99) / 100;
                for (int i = 0; i < currCount; i++) {
                    producer.send(new ProducerRecord<String, String>(topic, msg), stats.next(msg.length()));
                    count--;
                }
            }
        }
    }
}
