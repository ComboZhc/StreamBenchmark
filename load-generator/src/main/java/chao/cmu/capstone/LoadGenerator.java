package chao.cmu.capstone;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import com.twitter.hbc.twitter4j.handler.StatusStreamHandler;
import com.twitter.hbc.twitter4j.message.DisconnectMessage;
import com.twitter.hbc.twitter4j.message.StallWarningMessage;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.PartitionInfo;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class LoadGenerator {
    private static String CONSUMER_KEY = "3y2XFYQtooHvTrqX64a4VIhiu";
    private static String CONSUMER_SECRET = "yfb5vkZalapbYJEYCiQJRKgtW3NY2FWYiSnvCUiPibuHTkKtFa";
    private static String ACCESS_TOKEN = "287477012-X8NS8oXM5ojzWQiDQHTDRTfL9a78tZIGn3P0qoeI";
    private static String ACCESS_SECRET = "8h9bKnxqqsL3NbTP11fIIBQnmf4ewUbGzash96QdOj3Bz";
    private static KafkaProducer<String, String> producer;
    private static StatusListener listener = new StatusStreamHandler() {
        @Override
        public void onStatus(Status status) {
            System.out.println(status.getId());
            try {
                producer.send(new ProducerRecord<String, String>("test", status.toString())).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}

        @Override
        public void onTrackLimitationNotice(int limit) {}

        @Override
        public void onScrubGeo(long user, long upToStatus) {}

        @Override
        public void onStallWarning(StallWarning warning) {}

        @Override
        public void onException(Exception e) {}

        @Override
        public void onDisconnectMessage(DisconnectMessage message) {}

        @Override
        public void onStallWarningMessage(StallWarningMessage warning) {}

        @Override
        public void onUnknownMessageType(String s) {}
    };

    public static void main(String[] args) throws InterruptedException {


        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put("bootstrap.servers", args[0]);
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("client.id", "load-generator");
        producer = new KafkaProducer<String, String>(configs);

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
        Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET);
        BasicClient client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();
        ExecutorService service = Executors.newFixedThreadPool(2);
        Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(
                client, queue, Lists.newArrayList(listener), service);
        t4jClient.connect();
        t4jClient.process();
    }
}
