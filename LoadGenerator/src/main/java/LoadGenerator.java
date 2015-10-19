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
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class LoadGenerator {
    private static String CONSUMER_KEY = "3y2XFYQtooHvTrqX64a4VIhiu";
    private static String CONSUMER_SECRET = "yfb5vkZalapbYJEYCiQJRKgtW3NY2FWYiSnvCUiPibuHTkKtFa";
    private static String ACCESS_TOKEN = "287477012-X8NS8oXM5ojzWQiDQHTDRTfL9a78tZIGn3P0qoeI";
    private static String ACCESS_SECRET = "8h9bKnxqqsL3NbTP11fIIBQnmf4ewUbGzash96QdOj3Bz";
    private static StatusListener listener = new StatusStreamHandler() {
        @Override
        public void onStatus(Status status) {
            System.out.println(status.getCreatedAt().toString());
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

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        // Define our endpoint: By default, delimited=length is set (we need this for our processor)
        // and stall warnings are on.
        StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();

        Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET);
        // Authentication auth = new BasicAuth(username, password);

        // Create a new BasicClient. By default gzip is enabled.
        BasicClient client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        // Create an executor service which will spawn threads to do the actual work of parsing the incoming messages and
        // calling the listeners on each message
        int numProcessingThreads = 2;
        ExecutorService service = Executors.newFixedThreadPool(numProcessingThreads);

        // Wrap our BasicClient with the twitter4j client
        Twitter4jStatusClient t4jClient = new Twitter4jStatusClient(
                client, queue, Lists.newArrayList(listener), service);

        // Establish a connection
        t4jClient.connect();
        t4jClient.process();
    }
}
