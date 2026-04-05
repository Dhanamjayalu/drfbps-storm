package com.drfbps;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class FeatureSelectionBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // Just passing all features forward from Preprocessing to Prediction
        collector.emit(new Values(
                tuple.getDoubleByField("age"),
                tuple.getDoubleByField("ap_hi"),
                tuple.getDoubleByField("weight"),
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
        // These names MUST match what PredictionBolt reads
        declarer.declare(new Fields("age", "ap_hi", "weight", "chol", "gluc", "smoke", "active", "target", "source"));
    }
}