package chao.cmu.capstone;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class LoadGenerator {
    private static String CONSUMER_KEY = "3y2XFYQtooHvTrqX64a4VIhiu";
    private static String CONSUMER_SECRET = "yfb5vkZalapbYJEYCiQJRKgtW3NY2FWYiSnvCUiPibuHTkKtFa";
    private static String ACCESS_TOKEN = "287477012-X8NS8oXM5ojzWQiDQHTDRTfL9a78tZIGn3P0qoeI";
    private static String ACCESS_SECRET = "8h9bKnxqqsL3NbTP11fIIBQnmf4ewUbGzash96QdOj3Bz";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: <brokers> <topic>");
            System.exit(-1);
        }
        String brokers = args[0];
        String topic = args[1];
        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put("bootstrap.servers", brokers);
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("client.id", "load-generator");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET);
        BasicClient client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();
        client.connect();
        int count = 0;
        while (!client.isDone()) {
            try {
                String msg = queue.take();
                JSONObject json = new JSONObject(msg);
                if (!json.isNull("text")) {
                    producer.send(new ProducerRecord<String, String>(topic, msg)).get();
                    ++count;
                    if (count % 100 == 0) {
                        System.out.println("Emitted " + count + " tweets");
                    }
                }
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }
}
