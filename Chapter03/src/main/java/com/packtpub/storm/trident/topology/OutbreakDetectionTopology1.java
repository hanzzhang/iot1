package com.packtpub.storm.trident.topology;

import java.io.FileReader;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.microsoft.eventhubs.spout.EventHubSpoutConfig;
import com.microsoft.eventhubs.trident.OpaqueTridentEventHubSpout;
import com.packtpub.storm.trident.operator.*;
import com.packtpub.storm.trident.spout.DiagnosisEventSpout;
import com.packtpub.storm.trident.state.OutbreakTrendFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;

public class OutbreakDetectionTopology1 {

    public static void main(String[] args) throws Exception {
		EventHubSpoutConfig spoutConfig = readEHConfig(args);
		StormTopology topology = buildTopology(spoutConfig);

		Config config = new Config();
		//config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1L);
		if ((args != null) && (args.length > 0)) {
			config.setNumWorkers(spoutConfig.getPartitionCount());
			StormSubmitter.submitTopology(args[0], config, topology);
		} else {
			//config.setMaxTaskParallelism(2);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("test", config, topology);
			Thread.sleep(5000000L);
			localCluster.shutdown();
		}
    }
    
	static EventHubSpoutConfig readEHConfig(String[] args) throws Exception {
		EventHubSpoutConfig spoutConfig;
		Properties properties = new Properties();
		if (args.length > 1) {
			properties.load(new FileReader(args[1]));
		} else {
			properties.load(OutbreakDetectionTopology1.class.getClassLoader().getResourceAsStream("Config.properties"));
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
		System.out.println("  username: " + username);
		System.out.println("  password: " + password);
		System.out.println("  namespaceName: " + namespaceName);
		System.out.println("  entityPath: " + entityPath);
		System.out.println("  zkEndpointAddress: " + zkEndpointAddress);
			System.out.println("  partition count: " + partitionCount);
		System.out.println("  checkpoint interval: " + checkpointIntervalInSeconds);
		System.out.println("  receiver credits: " + receiverCredits);
		spoutConfig = new EventHubSpoutConfig(username, password, namespaceName, entityPath, partitionCount, zkEndpointAddress, checkpointIntervalInSeconds, receiverCredits);
		if (args.length > 0) {
			spoutConfig.setTopologyName(args[0]);
		}
		return spoutConfig;
	}

    static StormTopology buildTopology(EventHubSpoutConfig spoutConfig) {
        TridentTopology topology = new TridentTopology();
        
//		OpaqueTridentEventHubSpout spout = new OpaqueTridentEventHubSpout(spoutConfig);	
        DiagnosisEventSpout spout = new DiagnosisEventSpout();
        
        Stream inputStream = topology.newStream("message", spout);

        inputStream.each(new Fields("message"), new DiseaseFilter())
                .each(new Fields("message"), new CityAssignment(), new Fields("city"))
                .each(new Fields("message", "city"), new HourAssignment(), new Fields("hour", "cityDiseaseHour"))
                .groupBy(new Fields("cityDiseaseHour"))
                .persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count")).newValuesStream()
                .each(new Fields("cityDiseaseHour", "count"), new OutbreakDetector(), new Fields("alert"))
                .each(new Fields("alert"), new DispatchAlert(), new Fields());
        return topology.build();
    }
    
}