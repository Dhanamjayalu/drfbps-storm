package com.drfbps;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PerformanceBolt extends BaseBasicBolt {
    private long start = System.currentTimeMillis();
    private int count = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        count++;
        if (count % 1000 == 0) {
            double sec = (System.currentTimeMillis() - start) / 1000.0;
            System.out.printf("[PerformanceBolt] Total Processed: %d | Speed: %.1f rec/sec%n", count, count/sec);
        }
        // Emit all three fields to the next bolt
        collector.emit(new Values(tuple.getValue(0), tuple.getValue(1), tuple.getValue(2)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Must match the number of fields being emitted
        declarer.declare(new Fields("risk", "actual", "source"));
    }
}