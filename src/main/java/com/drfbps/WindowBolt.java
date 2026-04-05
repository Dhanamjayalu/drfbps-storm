package com.drfbps;

import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.*;
import org.apache.storm.topology.*;
import java.util.*;

public class WindowBolt extends BaseBasicBolt {
    private Queue<String> window = new LinkedList<>();
    private final int WINDOW_SIZE = 500;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String risk = tuple.getStringByField("risk");
        window.add(risk);
        if (window.size() > WINDOW_SIZE) window.poll();

        if (window.size() % 100 == 0) {
            long highCount = window.stream().filter(r -> r.equals("HIGH RISK")).count();
            double highPct = (highCount * 100.0) / window.size();
            System.out.printf("[WindowBolt] Sliding Trend → HIGH RISK: %.1f%% | LOW RISK: %.1f%%%n",
                    highPct, (100.0 - highPct));
        }
        // Emit all three values
        collector.emit(new Values(tuple.getValue(0), tuple.getValue(1), tuple.getValue(2)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("risk", "actual", "source"));
    }
}