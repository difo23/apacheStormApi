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
import com.mongodb.DBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankScenarySelectMTBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankScenarySelectMTBolt.class);
    private OutputCollector _collector;
    private MongoDBConnector _mongodb;



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this._collector = outputCollector;
        _mongodb = MongoDBConnector.getConnector(new ServerConnection(Config.Database.MT_CALL_SERVER_IP,
                Config.Database.MT_CALL_SERVER_PORT));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Inicio execute: ");

        String _input = tuple.getString(0);
        LOG.info(" Input: "+_input );
        BasicDBObject user = (BasicDBObject) JSON.parse(_input);
        BasicDBList allxdrs = new BasicDBList();

        try {



            Document where = new Document();
            where.append("msisdn", Long.valueOf((String) user.get("msisdn")));
            where.append("imsi", Long.valueOf((String) user.get("imsi")));
            where.append("imei",Long.valueOf((String) user.get("imei")));

            MongoCursor cursor = _mongodb.find(Config.Database.MT_CALL_XDR_DATABASE, Config.Database.MT_CALL_XDR_COLLECTION, where);
            //MongoCursor<Document> cursor = coll_mo.find( new Document("$or", asList)).iterator();
          //  MongoCursor<Document> cursor =coll_mo.find(or(eq("msisdn", user.get("msisdn")), eq("imei",
              //      user.get("imei")), eq("imsi", user.get("imsi")))).iterator();

            if(cursor== null){
                LOG.info("No existen xdrs corralados por escenarios para este usuario "+user.toString());
                _collector.ack(tuple);
               // this._collector.emit( new Values(" No existen datos corralados por escenarios! ", _retInfo));
            }
            else {
                while (cursor.hasNext()) {

                    BasicDBObject doc =(BasicDBObject) cursor.next();
                    if (doc == null) break;
                        /*
                        ObjectId id = (ObjectId) doc.get("_id");
                        BasicDBObject userselect = new  BasicDBObject("id_raw", id)
                                .append("msisdn", doc.get("msisdn"))
                                .append("imei", doc.get("imei"))
                                .append("imsi", doc.get("imsi"))
                                .append("xdr_raw", doc.get("xdr_raw"))
                                .append("end_time", doc.get("end_time") )
                                .append("start_time", doc.get("start_time") )
                                .append("scenarytype", Config.Database.MT_CALL_XDR_COLLECTION)
                                .append("mandatory", doc.get("mandatory"));*/

                            allxdrs.add(doc );
                }
                LOG.info(" Envio los datos corrralados para ser procesado por el algoritmo de tiempos "+allxdrs.toString());
                _collector.emit(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_SELECT_MT_TO_RECURSIVE_ALGORITM, tuple, new Values( JSON.serialize(allxdrs),  Config.Database.MT_CALL_XDR_COLLECTION) );
                _collector.ack(tuple);
            }

        }catch (Exception e) {
            //captura exception
            _collector.fail(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //outputFieldsDeclarer.declare( new Fields("result","return-info"));
        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_SELECT_MT_TO_RECURSIVE_ALGORITM, new Fields("grupos_by_user","scenary_type"));

    }
}
