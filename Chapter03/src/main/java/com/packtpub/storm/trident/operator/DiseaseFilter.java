package com.packtpub.storm.trident.operator;

import com.google.gson.Gson;
import com.packtpub.storm.trident.model.DiagnosisEvent;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class DiseaseFilter extends BaseFilter {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean isKeep(TridentTuple tuple) {
        //DiagnosisEvent diagnosis = (DiagnosisEvent) tuple.getValue(0);
        DiagnosisEvent diagnosis = (new Gson()).fromJson(tuple.getString(0), DiagnosisEvent.class);
        Integer code = Integer.parseInt(diagnosis.diagnosisCode);
        if (code.intValue() <= 322) {
            //LOG.debug("Emitting disease [" + code + "]");
            //System.out.println("Emitting disease [" + code + "]");
            return true;
        } else {
            //LOG.debug("Filtering disease [" + code + "]");
            return false;
        }
    }
}