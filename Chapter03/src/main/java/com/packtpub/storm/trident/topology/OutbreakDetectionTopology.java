package com.packtpub.storm.trident.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.packtpub.storm.trident.operator.*;
import com.packtpub.storm.trident.spout.DiagnosisEventSpout;
import com.packtpub.storm.trident.state.OutbreakTrendFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;

public class OutbreakDetectionTopology {

    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
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

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
		if (args.length == 0) {
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("cdc", conf, buildTopology());
	        Thread.sleep(200000);
	        cluster.shutdown();
		} else {
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}        
    }
}