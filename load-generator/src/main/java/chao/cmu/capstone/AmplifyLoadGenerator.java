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

public class AmplifyLoadGenerator {
    private final static Logger logger = LoggerFactory.getLogger(AmplifyLoadGenerator.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: <brokers> <topic> <amps>");
            System.out.println("<amps> could be a single number <amp>");
            System.out.println("<amps> could also be a time series of number <amp1>,<time1>,<amp2>,<time2>...");
            System.out.println("<amp> could be either a number or +/- number");
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

        KafkaProducer<String, String> producer = Utils.getKafkaProducer(brokers, AmplifyLoadGenerator.class.getName());
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        Client client = Utils.getTwitterClient(queue);
        client.connect();
        final WindowValue windowValue = new WindowValue(amps, times, 100);
        WindowStats stats = new WindowStats(1000);
        stats.addListener(new WindowListener() {
            @Override
            public void onWindow() {
                logger.info("amplify = {}", windowValue.getValue());
            }
        });
        boolean started = false;

        while (!client.isDone()) {
            try {
                String msg = queue.take();
                if (!started) {
                    started = true;
                    stats.start();
                    windowValue.start();
                }
                JSONObject json = new JSONObject(msg);
                if (!json.isNull("text")) {
                    for (int i = 0; i < windowValue.getValue(); i++) {
                        producer.send(new ProducerRecord<String, String>(topic, msg), stats.next(msg.length()));
                    }
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted", e);
            } catch (JSONException e) {
                logger.error("JSON Parse Error", e);
            }
        }
    }
}
