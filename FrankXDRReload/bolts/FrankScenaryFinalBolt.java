package com.ayscom.minetur.FrankXDRReload.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.ayscom.minetur.config.Config;
import com.ayscom.minetur.config.TopologiesIDs;
import com.mongodb.BasicDBList;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankScenaryFinalBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankScenaryFinalBolt.class);
    private OutputCollector _collector;





    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Inicio execute: ");
         String _input = tuple.getString(0);
        String _scenarytype = tuple.getString(1);
        LOG.info(" Input: "+_input );
        BasicDBList groups = (BasicDBList) JSON.parse(_input);
        this._collector.emit(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_FINAL_TO_SAVE_GROUP, tuple, new Values( JSON.serialize(groups)));
        this._collector.emit(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_FINAL_TO_DELETE_XDRS, tuple, new Values( JSON.serialize(groups), _scenarytype));
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_FINAL_TO_SAVE_GROUP, new Fields("grupos"));
        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_FINAL_TO_DELETE_XDRS, new Fields("grupos", "scenarytype"));

    }
}
