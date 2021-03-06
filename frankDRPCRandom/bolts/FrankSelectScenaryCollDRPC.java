package com.ayscom.minetur.frankDRPCRandom.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.ayscom.minetur.config.TopologiesIDs;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by lramirez on 6/08/15.
 */
public class FrankSelectScenaryCollDRPC extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FrankSelectScenaryCollDRPC.class);
    private OutputCollector collector;
    private Tuple tuple;
    private String token;
    private String input;
    private ObjectId id_frank;
    private  Object retInfo;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector= outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        LOG.info("Inicio execute: ");

        this.retInfo = tuple.getValue(1);
        this.input = tuple.getString(0);

        BasicDBObject user = (BasicDBObject) JSON.parse(input);
        this.collector.emit( new Values( JSON.serialize(user) ,this.retInfo));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result", "return-info"));

    }
}
