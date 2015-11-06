package chao.cmu.capstone;

import backtype.storm.generated.*;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class MetricTask {
    public static void printMetrics(Nimbus.Client client, String topologyName, String componentId) throws Exception {
        ClusterSummary summary = client.getClusterInfo();
        String id = null;
        for (TopologySummary ts: summary.get_topologies()) {
            if (topologyName.equals(ts.get_name())) {
                id = ts.get_id();
            }
        }
        TopologyInfo info = client.getTopologyInfo(id);
        int uptime = info.get_uptime_secs();
        long acked = 0;
        long failed = 0;
        double weightedAvgTotal = 0.0;
        for (ExecutorSummary exec: info.get_executors()) {
            if (componentId.equals(exec.get_component_id())) {
                SpoutStats stats = exec.get_stats().get_specific().get_spout();
                Map<String, Long> failedMap = stats.get_failed().get(":all-time");
                Map<String, Long> ackedMap = stats.get_acked().get(":all-time");
                Map<String, Double> avgLatMap = stats.get_complete_ms_avg().get(":all-time");
                for (String key: ackedMap.keySet()) {
                    if (failedMap != null) {
                        Long tmp = failedMap.get(key);
                        if (tmp != null) {
                            failed += tmp;
                        }
                    }
                    long ackVal = ackedMap.get(key);
                    double latVal = avgLatMap.get(key) * ackVal;
                    acked += ackVal;
                    weightedAvgTotal += latVal;
                }
            }
        }
        double avgLatency = weightedAvgTotal/acked;
        System.out.println("================================");
        System.out.println("uptime = " + uptime);
        System.out.println("acked = " + acked);
        System.out.println("avgLatency = " + avgLatency);
        System.out.println("failed = " + failed);
    }

    public static void main(String[] args) throws InterruptedException {
        final String topologyName = args[0];
        final String componentId = args[1];
        final Map clusterConf = Utils.readStormConfig();
        clusterConf.putAll(Utils.readCommandLineOpts());
        final Nimbus.Client client = NimbusClient.getConfiguredClient(clusterConf).getClient();
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    printMetrics(client, topologyName, componentId);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 0, 10 * 1000);
        Thread.sleep(24 * 3600 * 1000);
    }

}
