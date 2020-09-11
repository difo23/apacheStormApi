package com.ayscom.minetur.FrankXDRReload.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.ayscom.minetur.config.Config;
import com.ayscom.minetur.utils.MongoDBConnector;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankSaveTempGroupsBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankSaveTempGroupsBolt.class);
    private OutputCollector _collector;
    private MongoDBConnector _mongodb;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Inicio execute: ");

            LOG.info("Inicio execute: ");
            String _input = tuple.getString(0);
            LOG.info(" Input: "+_input );

            BasicDBList xdrs = (BasicDBList) JSON.parse(_input);
            ArrayList<BasicDBObject> groups= new ArrayList<>();

            try {
                for(Object group: xdrs){
                    BasicDBObject gr = (BasicDBObject) group;
                    groups.add(gr);
                }

                _mongodb.bulkInsert(Config.Database.GROUPS_TEMP_DATABASE, Config.Database.GROUPS_TEMP_COLLECTION, groups);
                _collector.ack(tuple);

        }catch (IllegalArgumentException e) {
            // Hacer algo con la Exception

        }

        // Save en group_temp
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

       //no emit
    }
}
