package com.microsoft.examples.bolts;

import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.google.gson.Gson;

import com.microsoft.examples.helpers.EventHubMessage;;

public class ParserBolt extends BaseBasicBolt {

  //Declare output fields & streams
  //hbasestream is all fields, and goes to hbase
  //dashstream is just the device and temperature, and goes to the dashboard
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("hdfsstream", new Fields("timestamp", "deviceid", "temperature"));
    //declarer.declareStream("dashstream", new Fields("deviceid", "temperature"));
  }

  //Process tuples
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Gson gson = new Gson();
    //Should only be one tuple, which is the JSON message from the spout
    String value = tuple.getString(0);

    //Convert it from JSON to an object
    EventHubMessage evMessage = gson.fromJson(value, EventHubMessage.class);

    //Pull out the values and emit as a stream
    String timestamp = evMessage.TimeStamp;
    int deviceid = evMessage.DeviceId;
    int temperature = evMessage.Temperature;
    collector.emit("hdfsstream", new Values(timestamp, deviceid, temperature));
    //collector.emit("dashstream", new Values(deviceid, temperature));
  }
}