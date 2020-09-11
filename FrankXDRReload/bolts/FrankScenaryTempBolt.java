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
 * Created by lramirez on 10/08/15.
 */
public class FrankScenaryTempBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankScenaryTempBolt.class);

    private OutputCollector _collector;
    private MongoDBConnector _mongodb;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this._collector = outputCollector;
        _mongodb = MongoDBConnector.getConnector(new ServerConnection(Config.Database.GROUPS_FINAL_SERVER_IP,
                Config.Database.GROUPS_FINAL_SERVER_PORT));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Inicio execute: ");
        String _input = tuple.getString(0);
        LOG.info(" Input: "+_input );

        BasicDBList xdrs = (BasicDBList) JSON.parse(_input);
        ArrayList<BasicDBObject> groupsdelete= new ArrayList<>();

        try {

         BasicDBObject xdr = (BasicDBObject) xdrs.get(0);

            Document where = new Document();
            where.append("msisdn", xdr.get("msisdn"))
                    .append("imsi", xdr.get("imsi"))
                    .append("scenarytype", xdr.get("scenarytype"))
                    .append("imei", xdr.get("imei"));

            BasicDBList results = _mongodb.findGroup(Config.Database.GROUPS_TEMP_DATABASE, Config.Database.GROUPS_TEMP_COLLECTION, where);

            for(Object group: results){
                BasicDBObject gr = (BasicDBObject) group;
                groupsdelete.add(gr);
            }
            _mongodb.bulkDelete(Config.Database.GROUPS_TEMP_DATABASE, Config.Database.GROUPS_TEMP_COLLECTION, groupsdelete);


        }catch (IllegalArgumentException e) {
            // Hacer algo con la Exception

        }

        this._collector.emit(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_TEMP_TO_SAVE_GROUP, tuple, new Values( JSON.serialize(xdrs)));
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_TEMP_TO_SAVE_GROUP, new Fields("grupos"));

    }
}
