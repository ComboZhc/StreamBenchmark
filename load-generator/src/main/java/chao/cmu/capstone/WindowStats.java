package chao.cmu.capstone;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class WindowStats extends TimerTask {
    private Logger logger = LoggerFactory.getLogger(WindowStats.class);
    private int totalCount = 0;
    private long totalLatency = 0;
    private int totalBytes = 0;
    private final Timer timer = new Timer();
    private final List<WindowListener> listeners = new LinkedList<>();

    public WindowStats() {
    }

    public void addListener(WindowListener listener) {
        listeners.add(listener);
    }

    public void removeListener(WindowListener listener) {
        listeners.remove(listener);
    }

    public void start() {
        timer.scheduleAtFixedRate(this, 0, 1000);
    }

    public synchronized void report(long latency, int bytes) {
        totalCount++;
        totalLatency += latency;
        totalBytes += bytes;
    }

    public Callback next(int bytes) {
        return new PerfCallback(this, bytes);
    }

    @Override
    public synchronized void run() {
        for (WindowListener listener : listeners)
            listener.onWindow();
        logger.info("Throughput = {} rec/s ({}KB/s) Avg Latency = {} ms Avg Payload = {} bytes ",
                totalCount,
                (float) totalBytes / 1024,
                (float) totalLatency / totalCount,
                (float) totalBytes / totalCount);
        totalCount = 0;
        totalLatency = 0;
        totalBytes = 0;
    }

    private class PerfCallback implements Callback {
        private final long start;
        private final WindowStats stats;
        private final int bytes;

        public PerfCallback(WindowStats stats, int bytes) {
            this.start = System.currentTimeMillis();
            this.stats = stats;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            this.stats.report(System.currentTimeMillis() - start, bytes);
            if (exception != null)
                logger.error("Send data error", exception);
        }
    }
}
