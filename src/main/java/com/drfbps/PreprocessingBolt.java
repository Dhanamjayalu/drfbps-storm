package com.drfbps;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PreprocessingBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        double ageYrs = tuple.getIntegerByField("age") / 365.25;
        double weight = tuple.getDoubleByField("weight");
        int ap_hi = tuple.getIntegerByField("ap_hi");

        // Clean BP outliers
        double cleanHi = (ap_hi > 250 || ap_hi < 40) ? 120.0 : (double) ap_hi;

        // Emit with correct types for FeatureSelection and Prediction
        collector.emit(new Values(
                ageYrs,
                weight,
                cleanHi,
                tuple.getIntegerByField("chol"),
                tuple.getIntegerByField("gluc"),
                tuple.getIntegerByField("smoke"),
                tuple.getIntegerByField("active"),
                tuple.getIntegerByField("target"),
                tuple.getStringByField("source")
        ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("age", "weight", "ap_hi", "chol", "gluc", "smoke", "active", "target", "source"));
    }
}