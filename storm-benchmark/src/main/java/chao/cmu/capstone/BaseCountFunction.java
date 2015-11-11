package chao.cmu.capstone;

import backtype.storm.metric.api.CountMetric;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class BaseCountFunction extends BaseFunction {
    transient CountMetric count;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        this.count = new CountMetric();
        context.registerMetric("message-count", this.count, 5);
    }

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        count.incr();
    }
}
