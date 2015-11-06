package chao.cmu.capstone;

import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AmplifyLoadGenerator {
    public static void main(String[] args) throws InterruptedException {
        if (args.length < 4) {
            System.out.println("Usage: <brokers> <topic> <time> <amps>");
            System.out.println("<amps> could be a single number <amp>");
            System.out.println("<amps> could also be a time series of number <amp1>,<time1>,<amp2>,<time2>...");
            System.out.println("<amp> could be either a number or +/- number");
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
            System.out.println("<amps> expects a single number or an even number of numbers");
            System.exit(-1);
        }
        int lastAmp = 0;
        int[] amps = new int[symbols.length / 2];
        long[] times = new long[symbols.length / 2];
        for (int i = 0; i < symbols.length / 2; i++) {
            String amp = symbols[i * 2];
            int currAmp;
            switch (amp.charAt(0)) {
                case '+':
                    currAmp = lastAmp + Integer.valueOf(amp.substring(1));
                    break;
                case '-':
                    currAmp = lastAmp - Integer.valueOf(amp.substring(1));
                    break;
                default:
                    currAmp = Integer.valueOf(amp);
                    break;
            }
            amps[i] = currAmp;
            lastAmp = currAmp;
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
        TimedValue timedValue = new TimedValue(amps, times, stats, "Amplification");
        boolean started = false;

        String msg = queue.take();
        stats.start();
        timedValue.start();
        while (!stats.stopped()) {
            if (Utils.isTweet(msg)) {
                stats.onSource();
                for (int i = 0; i < timedValue.getValue(); i++) {
                    producer.send(new ProducerRecord<String, String>(topic, msg), stats.onSend(msg.length()));
                }
            }
            msg = queue.take();
        }
        stats.printTotal();
        timedValue.stop();
        client.stop();
        producer.close();
    }
}
