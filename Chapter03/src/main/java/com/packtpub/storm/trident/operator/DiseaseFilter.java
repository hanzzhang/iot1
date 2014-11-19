package com.packtpub.storm.trident.operator;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.gson.Gson;
import com.packtpub.storm.trident.model.DiagnosisEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class DiseaseFilter extends BaseFilter {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DiseaseFilter.class);

    @Override
    public boolean isKeep(TridentTuple tuple) {
        //DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
        DiagnosisEvent diagnosis = (new Gson()).fromJson(tuple.getString(0), DiagnosisEvent.class);
        Integer code = Integer.parseInt(diagnosis.diagnosisCode);
        if (code.intValue() <= 322) {
            LOG.debug("Emitting disease [" + code + "]");
            return true;
        } else {
            LOG.debug("Filtering disease [" + code + "]");
            return false;
        }
    }
}