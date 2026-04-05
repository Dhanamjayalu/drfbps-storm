package com.drfbps;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class DRFBPSTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("health-spout", new HealthDataSpout());
        builder.setBolt("preprocess-bolt", new PreprocessingBolt()).shuffleGrouping("health-spout");
        builder.setBolt("feature-bolt", new FeatureSelectionBolt()).shuffleGrouping("preprocess-bolt");
        builder.setBolt("prediction-bolt", new PredictionBolt()).shuffleGrouping("feature-bolt");
        builder.setBolt("metrics-bolt", new MetricsAndSHAPBolt()).shuffleGrouping("prediction-bolt");
        builder.setBolt("window-bolt", new WindowBolt()).shuffleGrouping("prediction-bolt");
        builder.setBolt("performance-bolt", new PerformanceBolt()).shuffleGrouping("prediction-bolt");

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        System.out.println("\n>>> [SYSTEM] TOPOLOGY STARTING. PROCESSING 70K RECORDS + LIVE STREAM...");
        cluster.submitTopology("HeartAttackRiskTopology", conf, builder.createTopology());

        // Run for 3 minutes (180,000ms) to ensure Generated Records > 70,000
        Thread.sleep(180000);

        System.out.println("\n>>> [SYSTEM] TIME LIMIT REACHED. GENERATING FINAL PERFORMANCE REPORT...");

        // This triggers the cleanup() method in MetricsAndSHAPBolt
        cluster.shutdown();

        System.out.println(">>> [SYSTEM] SHUTDOWN COMPLETE. SCROLL UP FOR THE REPORT BOX.");
        System.exit(0);
    }
}