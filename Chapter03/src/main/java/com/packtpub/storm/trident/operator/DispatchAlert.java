package com.packtpub.storm.trident.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DispatchAlert extends BaseFunction {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String alert = (String) tuple.getValue(0);
        Logger logger = (Logger) LoggerFactory.getLogger(DispatchAlert.class);
      logger.info("Hanz ALERT RECEIVED [" + alert + "]");
      logger.info("Hanz Dispatch the national guard!");
        
        //System.exit(0);
    }
}
