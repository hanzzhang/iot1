package com.packtpub.storm.trident.topology;

import java.io.FileReader;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.microsoft.eventhubs.spout.EventHubSpoutConfig;
import com.microsoft.eventhubs.trident.OpaqueTridentEventHubSpout;
import com.packtpub.storm.trident.operator.*;
import com.packtpub.storm.trident.spout.DiagnosisEventSpout;

import redis.clients.jedis.Jedis;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class BlobWriterTopology{
	public static void main(String[] args) throws Exception {
		//flushRedisDB();
		StormTopology stormTopology = buildStormTopology(args);

		if ((args != null) && (args.length > 0)) {
			// if running in storm cluster
			Config config = new Config();
			int numWorkers = getNumWorkers(args);
			config.setNumWorkers(numWorkers);
			System.out.println("Number of workers = " + numWorkers);
			// config.registerMetricsConsumer(LoggingMetricsConsumer.class, 1L);
			StormSubmitter.submitTopology(args[0], config, stormTopology);
		} else {
			// if running in local development environment
			Config config = new Config();
			config.setMaxTaskParallelism(2);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("localTopology", config, stormTopology);
			Thread.sleep(5000000L);
			localCluster.shutdown();
		}
	}

	static void flushRedisDB() {
		String host = "hanzredis1.redis.cache.windows.net";
		Jedis jedis = new Jedis(host, 6380, 3600, true); // host, port, timeout,isSSL
		jedis.auth("eQoMISLEQf7mwCDetcvIUT+P9WGGK9KGsdf7/UOGkTg=");
		jedis.connect();
		if (jedis.isConnected()) {
			jedis.flushDB();
		} else {
			System.out.println("connection error");
		}
		jedis.close();
	}

	static StormTopology buildStormTopology(String[] args) throws Exception {
		Properties properties = new Properties();
		if ((args != null) && args.length > 1 && !args[1].toLowerCase().equals("test")) {
			System.out.println("Loding config file from file " + args[1]);
			properties.load(new FileReader(args[1]));
		} else {
			System.out.println("Loding config file from Config.properties");
			properties.load(OutbreakDetectionTopology1.class.getClassLoader().getResourceAsStream("Config.properties"));
		}

		
		TridentTopology tridentTopology = new TridentTopology();
		Stream inputStream = null;

		boolean useEventHubSpout = true;
		if ((args != null) && args.length > 1 && args[1].toLowerCase().equals("test")) {
			useEventHubSpout = false;
		}

		// useEventHubSpout = false;

		int numWorkers = getNumWorkers(args);

		if (useEventHubSpout) {
			System.out.println("useEventHubSpout = " + useEventHubSpout);
			OpaqueTridentEventHubSpout spout = createOpaqueTridentEventHubSpout(args);
			inputStream = tridentTopology.newStream("message", spout);
		} else {
			System.out.println("useEventHubSpout = " + useEventHubSpout);
			DiagnosisEventSpout spout = new DiagnosisEventSpout();
			inputStream = tridentTopology.newStream("message", spout);
		}

		inputStream
		.parallelismHint(numWorkers)
		.partitionAggregate(new Fields("message"),new ByteAggregator(properties), new Fields("blobname"));

		return tridentTopology.build();
	}

	static OpaqueTridentEventHubSpout createOpaqueTridentEventHubSpout(String[] args) throws Exception {
		EventHubSpoutConfig spoutConfig = readEHConfig(args);
		OpaqueTridentEventHubSpout spout = new OpaqueTridentEventHubSpout(spoutConfig);
		return spout;
	}

	static EventHubSpoutConfig readEHConfig(String[] args) throws Exception {
		EventHubSpoutConfig spoutConfig;
		Properties properties = new Properties();
		if ((args != null) && args.length > 1 && !args[1].toLowerCase().equals("test")) {
			System.out.println("Loding config file from file " + args[1]);
			properties.load(new FileReader(args[1]));
		} else {
			System.out.println("Loding config file from Config.properties");
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
		if ((args != null) && args.length > 0) {
			spoutConfig.setTopologyName(args[0]);
		}
		return spoutConfig;
	}

	static int getNumWorkers(String[] args) throws Exception {
		Properties properties = new Properties();
		if ((args != null) && args.length > 1 && !args[1].toLowerCase().equals("test")) {
			// read properties from the file specified by storm command line
			properties.load(new FileReader(args[1]));
		} else {
			// read properties from the Config.properties file
			properties.load(OutbreakDetectionTopology1.class.getClassLoader().getResourceAsStream("Config.properties"));
		}
		return Integer.parseInt(properties.getProperty("eventhubspout.partitions.count"));
	}

}