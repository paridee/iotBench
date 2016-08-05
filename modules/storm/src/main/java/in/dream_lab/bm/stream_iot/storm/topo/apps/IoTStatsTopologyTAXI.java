package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */

import in.dream_lab.bm.stream_iot.storm.PrometheusConsumer;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt.*;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleRedisSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;


/**
 * Created by anshushukla on 18/05/15.
 */
public class IoTStatsTopologyTAXI {

    public static void main(String[] args) throws Exception {
        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }

        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
        String taskPropFilename=argumentClass.getTasksPropertiesFilename();
        System.out.println("taskPropFilename-"+taskPropFilename);


        Config conf = new Config();
        conf.setDebug(false);
        conf.registerMetricsConsumer(PrometheusConsumer.class,1);
        conf.put("topology.backpressure.enable",false);

        conf.setNumWorkers(8);


        Properties p_=new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);


        TopologyBuilder builder = new TopologyBuilder();

//        System.out.println("Input File path name-"+argumentClass.getInputDatasetPathName());
//        builder.setSpout("spout1", new SampleSpout(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()),
//                1);

        String basePathForMultipleSpout="/home/paride/benchdataset/";

        System.out.println("basePathForMultipleSpout is used -"+basePathForMultipleSpout);

        String spout1InputFilePath=basePathForMultipleSpout+"datasetTAXI.csv";
        //String spout1InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-10spouts200mps-480sec-file1.csv";
        //String spout2InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-10spouts200mps-480sec-file2.csv";
        //String spout3InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-10spouts200mps-480sec-file3.csv";
        //String spout4InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-10spouts200mps-480sec-file4.csv";
        //String spout5InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-10spouts200mps-480sec-file5.csv";
        //String spout6InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-10spouts200mps-480sec-file6.csv";
        //String spout7InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-10spouts200mps-480sec-file7.csv";
        //String spout8InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-10spouts200mps-480sec-file8.csv";
        //String spout9InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-10spouts200mps-480sec-file9.csv";
        //String spout10InputFilePath=basePathForMultipleSpout+"TAXI-inputcsv-10spouts200mps-480sec-file10.csv";
        builder.setSpout("spout1", new SampleRedisSpout(),3);
        /* builder.setSpout("spout1", new SampleSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);
    /*    builder.setSpout("spout2", new SampleSpout(spout2InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);
        builder.setSpout("spout3", new SampleSpout(spout3InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);
        builder.setSpout("spout4", new SampleSpout(spout4InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);
        builderh.setSpout("spout5", new SampleSpout(spout5InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);
        builder.setSpout("spout6", new SampleSpout(spout6InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);
        builder.setSpout("spout7", new SampleSpout(spout7InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);
        builder.setSpout("spout8", new SampleSpout(spout8InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);
        builder.setSpout("spout9", new SampleSpout(spout9InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);
        builder.setSpout("spout10", new SampleSpout(spout10InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);*/

        builder.setBolt("ParseProjectTAXIBolt",
                new ParseProjectTAXIBolt(p_), 4)
                .setNumTasks(32)
                .shuffleGrouping("spout1")/*
                .shuffleGrouping("spout2")
                .shuffleGrouping("spout3")
                .shuffleGrouping("spout4")
                .shuffleGrouping("spout5")
                .shuffleGrouping("spout6")
                .shuffleGrouping("spout7")
                .shuffleGrouping("spout8")
                .shuffleGrouping("spout9")
                .shuffleGrouping("spout10")*/
        ;

        builder.setBolt("BloomFilterCheckBolt",
                new BloomFilterCheckBolt(p_), 4)
                .setNumTasks(32)
                .fieldsGrouping("ParseProjectTAXIBolt",new Fields("obsType")); // filed grouping on obstype

        builder.setBolt("KalmanFilterBolt",
                new KalmanFilterBolt(p_), 4)
                .setNumTasks(32)
                .fieldsGrouping("BloomFilterCheckBolt",new Fields("sensorID","obsType"));


        builder.setBolt("SimpleLinearRegressionPredictorBolt",
                new SimpleLinearRegressionPredictorBolt(p_), 4)
                .setNumTasks(32)
                .fieldsGrouping("KalmanFilterBolt",new Fields("sensorID","obsType"));

        builder.setBolt("BlockWindowAverageBolt",
                new BlockWindowAverageBolt(p_), 4)
                .setNumTasks(32)
                .fieldsGrouping("BloomFilterCheckBolt",new Fields("sensorID","obsType"));


        builder.setBolt("DistinctApproxCountBolt",
                new DistinctApproxCountBolt(p_), 4)
                .setNumTasks(32)
                .shuffleGrouping("BloomFilterCheckBolt");

        builder.setBolt("MQTTPublishTaskBolt",
                new MQTTPublishTaskBolt(p_), 13)
                .setNumTasks(32)
                .shuffleGrouping("SimpleLinearRegressionPredictorBolt")
                .shuffleGrouping("BlockWindowAverageBolt")
                .shuffleGrouping("DistinctApproxCountBolt");

        builder.setBolt("sink", new Sink(sinkLogFileName), 4)
                .setNumTasks(32)
                .shuffleGrouping("MQTTPublishTaskBolt");

//       builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("spout1");


        StormTopology stormTopology = builder.createTopology();

        if (argumentClass.getDeploymentMode().equals("C")) {
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
            Utils.sleep(100000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }
    }
}


// L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/TAXI-inputcsvSCTable-1spouts100mps-480sec.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test

// L   IdentityTopology   /Users/anshushukla/data/dataset-TAXI-12min-100x.csv     TAXI-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test



