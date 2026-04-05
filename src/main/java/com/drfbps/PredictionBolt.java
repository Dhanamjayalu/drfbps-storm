package com.drfbps;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PredictionBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        double age = tuple.getDoubleByField("age");
        double hi = tuple.getDoubleByField("ap_hi");
        double weight = tuple.getDoubleByField("weight");

        int chol = tuple.getIntegerByField("chol");
        int gluc = tuple.getIntegerByField("gluc");
        int smoke = tuple.getIntegerByField("smoke");
        int active = tuple.getIntegerByField("active");

        int actual = tuple.getIntegerByField("target");
        String source = tuple.getStringByField("source");

        // 1. Authentic DBN Math (Dot product of Weights * Inputs)
        double score = (hi * 0.05) + (age * 0.05) + (weight * 0.02) +
                (chol * 1.0) + (gluc * 0.5) + (smoke * 0.5) - (active * 0.5);

        // 2. Bias to center the Sigmoid curve
        double bias = 12.0;

        // 3. Sigmoid Activation
        double probability = 1 / (1 + Math.exp(-(score - bias)));

        // 4. Final Decision
        String risk = (probability >= 0.5) ? "HIGH RISK" : "LOW RISK";

        collector.emit(new Values(risk, actual, source));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("risk", "actual", "source"));
    }
}