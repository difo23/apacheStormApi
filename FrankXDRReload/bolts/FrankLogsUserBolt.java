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
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankLogsUserBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger( FrankLogsUserBolt.class);
    private OutputCollector _collector;
    private MongoDBConnector _mongodb;


    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
        _mongodb = MongoDBConnector.getConnector(new ServerConnection(Config.Database.MEASUREMENTS_SERVER_IP,
                Config.Database.MEASUREMENTS_SERVER_PORT));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Inicio execute: ");

        String _input = tuple.getString(0);
        String _scenarytype = tuple.getString(1);
        LOG.info(" Input: "+_input );

        BasicDBList groups = (BasicDBList) JSON.parse(_input);
        BasicDBList groups_final = new BasicDBList();
        BasicDBList groups_temp =  new BasicDBList();

        if(groups.size() > 0) {
                    for (Object group_object : groups) {


                        BasicDBObject group = (BasicDBObject) group_object;
                        Number msisdn = (Number) group.get("msisdn");
                        Number imei = (Number) group.get("imei");
                        Number imsi = (Number) group.get("imsi");
                        Date start_time = (Date) group.get("start_time");
                        Date end_time = (Date) group.get("end_time");

                        Document where = new Document();
                        where.append("msisdn", msisdn);
                        where.append("imsi", imsi);
                        where.append("imei", imei);
                        where.append("start_time", new Document("$lt", end_time));
                        where.append("end_time", new Document("$gt", start_time));

                        MongoCursor results = _mongodb.find(Config.Database.MEASUREMENTS_DATABASE, Config.Database.MEASUREMENTS_COLLECTION, where);
                        BasicDBList logs = new BasicDBList();
                        while (results.hasNext()) {
                            Document log = (Document) results.next();
                            logs.add(log);
                        }
                        group.append("logs", logs);
                        group.append("logs_correlados", logs.size());
                        group.append("scenaryid", new ObjectId());


                    Calendar cal = Calendar.getInstance();
                    cal.setTime( end_time);
                    cal.set(Calendar.HOUR, cal.get(Calendar.HOUR) + 4);
                    Date tempDate = cal.getTime();
                    LOG.info("Hora end time del grupo: " + group.get("end_time").toString());
                    LOG.info("Hora modificada: " + tempDate.toString());
                    Date now = new Date();

                    //Logica para grupos que poseen mas de 4 horas creandose
                    if (tempDate.getTime() < now.getTime()) {
                        groups_final.add(group);
                       // this._collector.emit(TopologiesIDs.FrankStreamNames.FRANK_LOGS_USER_TO_SCENARY_FINAL, tuple, new Values(JSON.serialize(group),_scenarytype));
                    } else {
                        groups_temp.add(group);
                       // this._collector.emit(TopologiesIDs.FrankStreamNames.FRANK_LOGS_USER_TO_SCENARY_TEMP, tuple, new Values(JSON.serialize(group)));
                    }
                   // _collector.ack(tuple);

            }

            this._collector.emit(TopologiesIDs.FrankStreamNames.FRANK_LOGS_USER_TO_SCENARY_FINAL, tuple, new Values(JSON.serialize( groups_final),_scenarytype));
            this._collector.emit(TopologiesIDs.FrankStreamNames.FRANK_LOGS_USER_TO_SCENARY_TEMP, tuple, new Values(JSON.serialize( groups_temp)));
            _collector.ack(tuple);
        }else{
            _collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_LOGS_USER_TO_SCENARY_FINAL, new Fields("grupos", "scenarytype"));
        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_LOGS_USER_TO_SCENARY_TEMP, new Fields("grupos"));

    }
}
