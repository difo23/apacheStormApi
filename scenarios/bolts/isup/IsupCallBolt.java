package com.ayscom.minetur.scenarios.bolts.isup;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.ayscom.minetur.XDR;
import com.ayscom.minetur.scenarios.bolts.ScenariosBolt;
import org.slf4j.Logger;
import org.bson.Document;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Created by Christian on 30/6/15.
 */
public class IsupCallBolt extends ScenariosBolt {

    OutputCollector _collector;
    private static final Logger LOG = LoggerFactory.getLogger(IsupCallBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare();
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Document document = (Document) tuple.getValue(0);
        XDR xdr = new XDR(document, "Calling Party Normalized", null, null, "Start Time", "End Time");
        XDR mo_call_xdr = xdr.clone();
        mo_call_xdr.setMandatory(true);
        insert_mo_call_xdr(mo_call_xdr);


        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
