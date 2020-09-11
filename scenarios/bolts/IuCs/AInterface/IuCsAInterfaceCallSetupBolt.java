package com.ayscom.minetur.scenarios.bolts.IuCs.AInterface;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ayscom.minetur.XDR;
import com.ayscom.minetur.config.Config;
import com.ayscom.minetur.scenarios.bolts.ScenariosBolt;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.util.Map;

/**
 * Created by Christian on 30/6/15.
 */
public class IuCsAInterfaceCallSetupBolt extends ScenariosBolt {

    OutputCollector _collector;

    static Logger LOG = Logger.getLogger(IuCsAInterfaceCallSetupBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare();
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Document document = (Document) tuple.getValue(0);
        XDR xdr = new XDR(document, "MSISDN", "IMEI", "IMSI", "Start Time", "End Time Correlation");
        if (xdr.getXDR_Raw().getString("Call Type").equals("Mobile Originated (MO) Call")) {
            XDR mo_call_xdr = xdr.clone();
            mo_call_xdr.setMandatory(true);
            insert_mo_call_xdr(mo_call_xdr);
        }
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
