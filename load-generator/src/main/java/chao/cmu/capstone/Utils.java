package chao.cmu.capstone;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class Utils {

    private static String CONSUMER_KEY = "3y2XFYQtooHvTrqX64a4VIhiu";
    private static String CONSUMER_SECRET = "yfb5vkZalapbYJEYCiQJRKgtW3NY2FWYiSnvCUiPibuHTkKtFa";
    private static String ACCESS_TOKEN = "287477012-X8NS8oXM5ojzWQiDQHTDRTfL9a78tZIGn3P0qoeI";
    private static String ACCESS_SECRET = "8h9bKnxqqsL3NbTP11fIIBQnmf4ewUbGzash96QdOj3Bz";

    public static Client getTwitterClient(BlockingQueue<String> queue) {
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET);
        BasicClient client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();
        return client;
    }

    public static KafkaProducer<String, String> getKafkaProducer(String brokers, String clientId) {
        Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", brokers);
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("client.id", clientId);
        return new KafkaProducer<>(configs);
    }

    public static boolean isTweet(String msg) {
        try {
            JSONObject object = new JSONObject(msg);
            return !object.isNull("text");
        } catch (JSONException e) {
            return false;
        }
    }
}
