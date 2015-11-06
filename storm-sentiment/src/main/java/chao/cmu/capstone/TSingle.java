package chao.cmu.capstone;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class TSingle {
    public static StormTopology createTopology(String zkhosts, String topic, int hint, String func) {
        TridentKafkaConfig config = new TridentKafkaConfig(
                new ZkHosts(zkhosts),
                topic,
                TSingle.class.getSimpleName() + Long.toString(System.currentTimeMillis()));
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        config.metricsTimeBucketSizeInSecs = 5;
        TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(config);

        BaseCountFunction function;
        function = new IdFunction();
        if (func.startsWith("p"))
            function = new ProjectFunction();
        if (func.startsWith("s"))
            function = new SampleFunction();
        if (func.startsWith("f"))
            function = new FilterFunction();

        TridentTopology topology = new TridentTopology();
        Stream s = topology.newStream("str", spout).name("spout")
                .each(new Fields("str"), function, new Fields("output")).parallelismHint(hint).name("bolt");
        if (func.endsWith("a"))
            s.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));
        return topology.build();
    }

    public static class IdFunction extends BaseCountFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            tridentCollector.emit(tridentTuple);
            super.execute(tridentTuple, tridentCollector);
        }
    }

    public static class ProjectFunction extends BaseCountFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String message = tridentTuple.getString(0);
            JSONObject object = (JSONObject) JSONValue.parse(message);
            String text = (String)object.get("text");
            tridentCollector.emit(new Values(text));
            super.execute(tridentTuple, tridentCollector);
        }
    }

    public static class SampleFunction extends BaseCountFunction {
        int sampleCount = 0;
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            if (sampleCount % 10 == 0) {
                tridentCollector.emit(tridentTuple);
            }
            sampleCount++;
            super.execute(tridentTuple, tridentCollector);
        }
    }

    public static class FilterFunction extends BaseCountFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String message = tridentTuple.getString(0);
            JSONObject object = (JSONObject) JSONValue.parse(message);
            if ("en".equals(object.get("lang"))) {
                tridentCollector.emit(tridentTuple);
            }
            super.execute(tridentTuple, tridentCollector);
        }
    }

    public static void main(String[] args) throws Exception {
        String zkHosts = args[0];
        String topic = args[1];
        int workers = Integer.valueOf(args[2]);
        int hint = Integer.valueOf(args[3]);
        String func = args[4];
        Config conf = new Config();
        conf.setNumWorkers(workers);
        conf.put(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 5);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);
        StormSubmitter.submitTopologyWithProgressBar(
                TSingle.class.getSimpleName(),
                conf,
                createTopology(zkHosts, topic, hint, func));
    }
}
