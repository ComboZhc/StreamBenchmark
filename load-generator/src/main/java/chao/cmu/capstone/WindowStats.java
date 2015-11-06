package chao.cmu.capstone;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.*;

public class WindowStats extends TimerTask {
    private int windowSource = 0;
    private int windowSent = 0;
    private int windowAcked = 0;
    private long windowLatency = 0;
    private int windowBytes = 0;
    private Map<String, Object> windowParameters = new HashMap<>();

    private int totalSource = 0;
    private int totalSent = 0;
    private int totalAcked = 0;
    private long totalLatency = 0;
    private int totalBytes = 0;

    private final Timer timer = new Timer();
    private long start = 0;
    private long duration;
    private boolean stopped = false;

    private final List<Listener> listeners = new LinkedList<>();

    public WindowStats(long duration) {
        this.duration = duration;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void start() {
        System.out.println("================================");
        start = System.currentTimeMillis();
        timer.scheduleAtFixedRate(this, 0, 1000);
    }

    public boolean stopped() {
        return stopped;
    }

    public synchronized void putParameter(String key, Object value) {
        windowParameters.put(key, value);
    }

    public synchronized void onSource() {
        windowSource++;
        totalSource++;
    }

    public synchronized void onAck(long latency, int bytes) {
        windowAcked++;
        windowLatency += latency;
        windowBytes += bytes;
        totalAcked++;
        totalLatency += latency;
        totalBytes += bytes;
    }

    public synchronized Callback onSend(int bytes) {
        windowSent++;
        totalSent++;
        return new WindowStatsCallback(this, bytes);
    }

    @Override
    public synchronized void run() {
        for (Listener listener : listeners)
            listener.onTick();
        System.out.println(String.format("Time = %d", System.currentTimeMillis()));
        System.out.println(String.format("Source (rec/s) = %d", windowSource));
        System.out.println(String.format("Sent (rec/s) = %d", windowSent));
        System.out.println(String.format("Throughput (rec/s) = %d", windowAcked));
        System.out.println(String.format("Throughput (MB/s) = %.3f", (float) windowBytes / 1024 / 1024));
        System.out.println(String.format("Avg Latency (ms) = %.3f", (float) windowLatency / windowAcked));
        System.out.println(String.format("Avg Payload (bytes) = %.1f", (float) windowBytes / windowAcked));
        for (Map.Entry<String, Object> entry : windowParameters.entrySet())
            System.out.println(String.format("%s = %s", entry.getKey(), entry.getValue()));
        System.out.println("--------------------------------");
        windowSource = 0;
        windowSent = 0;
        windowAcked = 0;
        windowLatency = 0;
        windowBytes = 0;
        windowParameters.clear();
        if (System.currentTimeMillis() > start + duration) {
            stopped = true;
            timer.cancel();
        }
    }

    public synchronized void printTotal() {
        System.out.println("================================");
        System.out.println(String.format("Duration = %d seconds", duration / 1000));
        System.out.println(String.format("Source (rec/s) = %.3f", totalSource / (duration / 1000f)));
        System.out.println(String.format("Sent (rec/s) = %.3f", totalSent / (duration / 1000f)));
        System.out.println(String.format("Throughput (rec/s) = %.3f", totalAcked / (duration / 1000f)));
        System.out.println(String.format("Throughput (MB/s) = %.3f", totalBytes / 1024f / 1024f / (duration / 1000f)));
        System.out.println(String.format("Avg Latency (ms) = %.3f", (float) totalLatency / totalAcked));
        System.out.println(String.format("Avg Payload (bytes) = %.1f", (float) totalBytes / totalAcked));
        System.out.println("********************************");
    }

    public interface Listener {
        public void onTick();
    }

    private class WindowStatsCallback implements Callback {
        private final long start;
        private final WindowStats stats;
        private final int bytes;

        public WindowStatsCallback(WindowStats stats, int bytes) {
            this.start = System.currentTimeMillis();
            this.stats = stats;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            this.stats.onAck(System.currentTimeMillis() - start, bytes);
            if (exception != null)
                exception.printStackTrace();
        }
    }
}
