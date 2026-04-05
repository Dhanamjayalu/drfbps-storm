package com.drfbps;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class MetricsAndSHAPBolt extends BaseBasicBolt {
    private int realCorrect = 0, realTotal = 0;
    private int genCorrect = 0, genTotal = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String pred = tuple.getStringByField("risk");
        int actual = tuple.getIntegerByField("actual");
        String source = tuple.getStringByField("source");

        boolean isCorrect = (pred.equals("HIGH RISK") && actual == 1) ||
                (pred.equals("LOW RISK") && actual == 0);

        if ("REAL_CSV".equals(source)) {
            realTotal++;
            if (isCorrect) realCorrect++;
        } else {
            genTotal++;
            if (isCorrect) genCorrect++;
        }

        // Print progress every 10,000 records
        if ((realTotal + genTotal) % 10000 == 0) {
            printSummary();
        }
    }

    @Override
    public void cleanup() {
        System.out.println("\n***************************************************");
        System.out.println("          FINAL PROJECT PERFORMANCE REPORT         ");
        printSummary();
        System.out.println("***************************************************\n");
    }

    private void printSummary() {
        if (realTotal > 0)
            System.out.printf(">>> [REAL DATASET]  Processed: %d | Accuracy: %.2f%%%n", realTotal, ((double)realCorrect/realTotal)*100);
        if (genTotal > 0)
            System.out.printf(">>> [GENERATED DATA] Processed: %d | Accuracy: %.2f%%%n", genTotal, ((double)genCorrect/genTotal)*100);
    }

    @Override public void declareOutputFields(OutputFieldsDeclarer d) {}
}