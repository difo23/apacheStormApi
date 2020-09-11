package com.ayscom.minetur.FrankXDRReload.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.ayscom.minetur.config.Config;
import com.ayscom.minetur.utils.MongoDBConnector;
import com.ayscom.minetur.utils.ServerConnection;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankScenarySelectMobilityBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankScenarySelectMobilityBolt.class);
    private OutputCollector _collector;
    private MongoDBConnector _mongodb;



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this._collector = outputCollector;
        _mongodb = MongoDBConnector.getConnector(new ServerConnection(Config.Database.MOBILITY_SERVER_IP,
                Config.Database.MOBILITY_SERVER_PORT));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Inicio execute: ");

        String _input = tuple.getString(0);
        LOG.info(" Input: "+_input );
        BasicDBObject user = ( BasicDBObject) JSON.parse(_input);
        BasicDBList allxdrs = new BasicDBList();

        try {
            Long msisdn= new Long(0);
            Long imsi= new Long(0);
            Long imei= new Long(0);
                if(user.get("msisdn")!=null){
                    msisdn=Long.valueOf((String) user.get("msisdn"));

                }
            if(user.get("imsi")!=null){
                imsi=Long.valueOf((String) user.get("imsi"));

            }
            if(user.get("imei")!=null){
               imei=Long.valueOf((String) user.get("imei"));

            }


            Document where = new Document();
            where.append("msisdn",(Number)  msisdn);
            where.append("imsi", imsi);
            where.append("imei", imei);
            MongoCursor cursor = _mongodb.find(Config.Database.MOBILITY_XDR_DATABASE, Config.Database.MOBILITY_XDR_COLLECTION, where);

            if(cursor== null){
                LOG.info("No existen xdrs para este usuario en este escenario"+user.toString());
                _collector.ack(tuple);
            }
            else {
                while (cursor.hasNext()) {

                    BasicDBObject doc =( BasicDBObject) cursor.next();
                    if (doc == null) break;
                        /*
                        ObjectId id = (ObjectId) doc.get("_id");
                 