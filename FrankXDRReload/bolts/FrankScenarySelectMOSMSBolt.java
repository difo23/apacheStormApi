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

import java.util.Map;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.or;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankScenarySelectMOSMSBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankScenarySelectMOSMSBolt.class);
    private OutputCollector _collector;
    private MongoDBConnector _mongodb;



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this._collector = outputCollector;
        _mongodb = MongoDBConnector.getConnector(new ServerConnection(Config.Database.MO_SMS_SERVER_IP,
                Config.Database.MO_SMS_SERVER_PORT));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Inicio execute: ");

        String _input = tuple.getString(0);
        LOG.info(" Input: "+_input );
        BasicDBObject user = (BasicDBObject) JSON.parse(_input);
        BasicDBList allxdrs = new BasicDBList();

        try {

            Object msisdn= new Object();
            Object imsi= new Object();
            Object imei= new Object();

            if(user.get("msisdn")!=null){
                msisdn=  user.get("msisdn");
                LOG.info(" MSISDN:"+msisdn);


            }
            if(user.get("imsi")!=null){
                imsi= user.get("imsi");
                LOG.info(" IMSI:"+ imsi);
            }
            if(user.get("imei")!=null){
                imei= user.get("imei");
                LOG.info(" IMEI:"+imei);
            }



            Document msisdn1 = new Document("msisdn", (long) msisdn);
            Document imsi1 = new Document("imsi", (long)   imsi);
            Document imei1 = new Document("imei",   (long) imei);
            BasicDBList lis = new BasicDBList();
            lis.add(msisdn1);
            lis.add(imsi1);
            lis.add(imei1);





                    //MongoCursor cursor = _mongodb.find(Config.Database.MO_SMS_XDR_DATABASE,  Config.Database.MO_SMS_XDR_COLLECTION, where);

            MongoClient mongo = new MongoClient(new ServerAddress("localhost", 27017));
            final CreateCollectionOptions options = new CreateCollectionOptions();
            options.capped(true);
            options.sizeInBytes(10000);
            final MongoCollection coll_mo = mongo.getDatabase("ministerio").getCollection("mo_call_xdrs_v2");
             //MongoCursor<BasicDBObject> cursor =coll_mo.find(or(eq("msisdn", msisdn), eq("imei", imei), eq("imsi", imsi))).iterator();
            Document whereor= new Document("$or", lis);

            LOG.info(" whereor: "+whereor.toJson());

            MongoCursor<BasicDBObject> cursor = coll_mo.find( new Document("$or", lis)).iterator();

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
                                .append("scenarytype", Config.Database.MO_SMS_XDR_COLLECTION )
                                .append("mandatory", doc.get("mandatory"));
                                */
                    LOG.info(" Xdrs devueltos por el cursor " + doc.toString());
                            allxdrs.add(doc);
                }
                LOG.info(" Envio los datos corrralados para ser procesado por el algoritmo de tiempos "+allxdrs.toString());
                _collector.emit(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_SELECT_MO_SMS_TO_RECURSIVE_ALGORITM, tuple, new Values( JSON.serialize(allxdrs),  Config.Database.MO_SMS_XDR_COLLECTION) );
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
        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_SELECT_MO_SMS_TO_RECURSIVE_ALGORITM, new Fields("grupos_by_user","scenary_type"));

    }
}
