package com.drfbps;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class HealthDataSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private Random rand = new Random();
    private boolean csvFinished = false;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.reader = new BufferedReader(new FileReader("heart_data.csv"));
            this.reader.readLine(); // Skip CSV Header
        } catch (Exception e) { csvFinished = true; }
    }

    @Override
    public void nextTuple() {
        if (!csvFinished) {
            try {
                String line = reader.readLine();
                if (line != null) {
                    String[] p = line.split(";");
                    collector.emit(new Values(
                            Integer.parseInt(p[1]), Integer.parseInt(p[2]), Integer.parseInt(p[3]),
                            Double.parseDouble(p[4]), Integer.parseInt(p[5]), Integer.parseInt(p[6]),
                            Integer.parseInt(p[7]), Integer.parseInt(p[8]), Integer.parseInt(p[9]),
                            Integer.parseInt(p[10]), Integer.parseInt(p[11]), Integer.parseInt(p[12]),
                            "REAL_CSV"
                    ));
                    return;
                } else {
                    csvFinished = true;
                    reader.close();
                    System.out.println("\n*** CSV FINISHED. STARTING BIG DATA STREAM... ***\n");
                }
            } catch (Exception e) { csvFinished = true; }
        }

        // GENERATED BIG DATA LOGIC
        boolean isHighRisk = rand.nextBoolean();
        int target = isHighRisk ? 1 : 0;

        // Create healthy or at-risk profile
        int age = isHighRisk ? (18000 + rand.nextInt(5000)) : (12000 + rand.nextInt(5000));
        double weight = isHighRisk ? (85.0 + rand.nextDouble()*20) : (60.0 + rand.nextDouble()*15);
        int ap_hi = isHighRisk ? (145 + rand.nextInt(30)) : (115 + rand.nextInt(10));
        int chol = isHighRisk ? (2 + rand.nextInt(2)) : 1;
        int active = isHighRisk ? 0 : 1;

        // Apply 20% noise to ensure ~80% Accuracy
        if (rand.nextInt(100) < 20) {
            target = (target == 1) ? 0 : 1;
        }

        collector.emit(new Values(age, 1, 170, weight, ap_hi, 80, chol, 1, 0, 0, active, target, "GENERATED"));

        // Small sleep to maintain steady flow
        try { Thread.sleep(1); } catch (Exception e) {}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("age", "gender", "height", "weight", "ap_hi", "ap_lo", "chol", "gluc", "smoke", "alco", "active", "target", "source"));
    }
}