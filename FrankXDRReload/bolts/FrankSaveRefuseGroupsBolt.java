package com.ayscom.minetur.FrankXDRReload.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ayscom.minetur.FrankXDRReload.FrankUtilities.FrankUtils;
import com.ayscom.minetur.config.Config;
import com.ayscom.minetur.utils.MongoDBConnector;
import com.ayscom.minetur.utils.ServerConnection;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
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
 * Created by lramirez on 13/08/15.
 */
public class FrankSaveRefuseGroupsBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankSaveRefuseGroupsBolt.class);

    private OutputCollector _collector;
    private MongoDBConnector _mongodb;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this._collector = outputCollector;
        _mongodb = MongoDBConnector.getConnector(new ServerConnection(Config.Database.GROUPS_REFUSE_SERVER_IP,
                Config.Database.GROUPS_REFUSE_SERVER_PORT));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Inicio execute: ");
        String _input = tuple.getString(0);
        LOG.info(" Input: "+_input );


        ArrayList<BasicDBObject> _documents= new  ArrayList<>() ;
        BasicDBList groups = (BasicDBList) JSON.parse(_input);


        try {

            if(groups.size() > 0) {
                for (Object group_object : groups) {
                    BasicDBObject group = (BasicDBObject) group_object;
                    _documents.add(group);

                }
                _mongodb.bulkInsert(Config.Database.GROUPS_REFUSE_DATABASE,Config.Database.GROUPS_REFUSE_COLLECTION, _documents );
                _collector.ack(tuple);
            }

        }catch (IllegalArgumentException e) {
            // Hacer algo con la Exception

        }

        //Save en BD group_final
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        //No emit
    }
}
