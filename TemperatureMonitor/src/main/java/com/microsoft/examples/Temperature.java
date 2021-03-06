package com.microsoft.examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.microsoft.eventhubs.spout.EventHubSpout;
import com.microsoft.eventhubs.spout.EventHubSpoutConfig;

import java.io.FileReader;
import java.util.Properties;

//hbase
//import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
//import org.apache.storm.hbase.bolt.HBaseBolt;

//hdfs


import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import com.microsoft.examples.bolts.ParserBolt;

public class Temperature
{
  protected EventHubSpoutConfig spoutConfig;
  protected int numWorkers;

  // Reads the configuration information for the Event Hub spout
  protected void readEHConfig(String[] args) throws Exception {
    Properties properties = new Properties();
    if(args.length > 1) {
      properties.load(new FileReader(args[1]));
    }
    else {
			properties.load(Temperature.class.getClassLoader().getResourceAsStream("Config.properties"));
    }

    String username = properties.getProperty("eventhubspout.username");
    String password = properties.getProperty("eventhubspout.password");
    String namespaceName = properties.getProperty("eventhubspout.namespace");
    String entityPath = properties.getProperty("eventhubspout.entitypath");
    String zkEndpointAddress = properties.getProperty("zookeeper.connectionstring");
    int partitionCount = Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
    int checkpointIntervalInSeconds = Integer.parseInt(properties.getProperty("eventhubspout.checkpoint.interval"));
    int receiverCredits = Integer.parseInt(properties.getProperty("eventhub.receiver.credits"));
    System.out.println("Eventhub spout config: ");
    System.out.println("  partition count: " + partitionCount);
    System.out.println("  checkpoint interval: " + checkpointIntervalInSeconds);
    System.out.println("  receiver credits: " + receiverCredits);
    System.out.println("   username: " + username);
    System.out.println("   namespace: " + namespaceName);
    System.out.println("   entitypath: " + entityPath);
    System.out.println("   zookeeper: " + zkEndpointAddress);
    spoutConfig = new EventHubSpoutConfig(username, password,
      namespaceName, entityPath, partitionCount, zkEndpointAddress,
      checkpointIntervalInSeconds, receiverCredits);
    System.out.println("   spoutConfig created: ");

    //set the number of workers to be the same as partition number.
    //the idea is to have a spout and a partial count bolt co-exist in one
    //worker to avoid shuffling messages across workers in storm cluster.
    numWorkers = spoutConfig.getPartitionCount();
    System.out.println("   numWorkers: " + numWorkers);

    if(args.length > 0) {
      //set topology name so that sample Trident topology can use it as stream name.
      System.out.println("   Set Topology Name: " + args[0]);
      spoutConfig.setTopologyName(args[0]);
    }
  }

  // Create the spout using the configuration
  protected EventHubSpout createEventHubSpout() {
    EventHubSpout eventHubSpout = new EventHubSpout(spoutConfig);
    return eventHubSpout;
  }

  // Build the topology
  //protected StormTopology buildTopology(EventHubSpout eventHubSpout, SimpleHBaseMapper mapper) {
  protected StormTopology buildTopology(EventHubSpout eventHubSpout) {
    TopologyBuilder topologyBuilder = new TopologyBuilder();
    // Name the spout 'EventHubsSpout', and set it to create
    // as many as we have partition counts in the config file
    topologyBuilder.setSpout("EventHub", eventHubSpout, spoutConfig.getPartitionCount())
      .setNumTasks(spoutConfig.getPartitionCount());
    // Create the parser bolt, which subscribes to the stream from EventHub
    topologyBuilder.setBolt("Parser", new ParserBolt(), spoutConfig.getPartitionCount())
      .localOrShuffleGrouping("EventHub").setNumTasks(spoutConfig.getPartitionCount());

    // Create the dashboard bolt, which subscribes to the stream from Parser
    //topologyBuilder.setBolt("Dashboard", new DashboardBolt(), spoutConfig.getPartitionCount())
    //  .fieldsGrouping("Parser", "dashstream", new Fields("deviceid")).setNumTasks(spoutConfig.getPartitionCount());

 // Use pipe as record boundary
    RecordFormat format = new DelimitedRecordFormat()
        .withFieldDelimiter("|");

    //Synchronize data buffer with the filesystem every 10 tuples
    SyncPolicy syncPolicy = new CountSyncPolicy(10);

    // Rotate data files when they reach five MB
    FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

    // Use default, Storm-generated file names
    FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/foo");

    // Instantiate the HdfsBolt
    HdfsBolt bolt = new HdfsBolt()
             .withFsUrl("hdfs://headnodehost:9000")
             //.withFsUrl("wasb://hanzstorm2@hanzstorage1.blob.core.windows.net/aaastorm2")
		     //the above one throws: 
             //java.lang.RuntimeException: Error preparing HdfsBolt: No FileSystem for scheme: wasb at org.apache.storm.hdfs.bolt.AbstractHdfsBolt.prepare(AbstractHdfsBolt.java:96) at backtype.storm.daemon.execu
             .withFileNameFormat(fileNameFormat)
             .withRecordFormat(format)
             .withRotationPolicy(rotationPolicy)
             .withSyncPolicy(syncPolicy);

    topologyBuilder.setBolt("Hdfs", bolt, spoutConfig.getPartitionCount())
        .fieldsGrouping("Parser", "hdfsstream", new Fields("deviceid"))
        .setNumTasks(spoutConfig.getPartitionCount());

    // Create the HBase bolt, which subscribes to the stream from Parser
    // WARNING - uncomment the following two lines when deploying
    // leave commented when testing locally
    // topologyBuilder.setBolt("HBase", new HBaseBolt("SensorData", mapper).withConfigKey("hbase.conf"), spoutConfig.getPartitionCount())
    //  .fieldsGrouping("Parser", "hbasestream", new Fields("deviceid")).setNumTasks(spoutConfig.getPartitionCount());
    return topologyBuilder.createTopology();
  }

  protected void submitTopology(String[] args, StormTopology topology, Config config) throws Exception {
    // Config config = new Config();
    config.setDebug(false);

    //Enable metrics
    config.registerMetricsConsumer(backtype.storm.metric.LoggingMetricsConsumer.class, 1);

    // Is this running locally, or on an HDInsight cluster?
    if (args != null && args.length > 0) {
      config.setNumWorkers(numWorkers);
      StormSubmitter.submitTopology(args[0], config, topology);
    } else {
      config.setMaxTaskParallelism(2);

      LocalCluster localCluster = new LocalCluster();
      localCluster.submitTopology("test", config, topology);

      Thread.sleep(5000000);

      localCluster.shutdown();
    }
  }

  // Loads the configuration, creates the spout, builds the topology,
  // and then submits it
  protected void runScenario(String[] args) throws Exception{
    readEHConfig(args);
    System.out.println("   readEHConfig succeeded ");
    Config config = new Config();

    //hbase configuration
    //Map<String, Object> hbConf = new HashMap<String, Object>();
    //if(args.length > 0) {
    //  hbConf.put("hbase.rootdir", args[0]);
    //}
    //config.put("hbase.conf", hbConf);
    //SimpleHBaseMapper mapper = new SimpleHBaseMapper()
    //      .withRowKeyField("deviceid")
    //      .withColumnFields(new Fields("timestamp", "temperature"))
    //      .withColumnFamily("cf");

    EventHubSpout eventHubSpout = createEventHubSpout();
    //StormTopology topology = buildTopology(eventHubSpout, mapper);
    StormTopology topology = buildTopology(eventHubSpout);
    submitTopology(args, topology, config);
  }

  public static void main(String[] args) throws Exception {
    System.out.println("   main started: ");

    Temperature scenario = new Temperature();
    
    scenario.runScenario(args);
  }
}