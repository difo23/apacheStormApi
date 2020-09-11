package com.ayscom.minetur.frankDRPCRandom.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.ayscom.minetur.config.TopologiesIDs;
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

/**
 * Created by lizandro on 2/08/15.
 */
public class FrankFilterUserDRPC extends BaseRichBolt {


    private OutputCollector collector;
    private static final Logger LOG = LoggerFactory.getLogger( FrankFilterUserDRPC.class );
    private String streamname = new String("frankinitDRPC");

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        String input = tuple.getString(0);
        DBObject user = (DBObject) JSON.parse(input);
        Object retInfo = tuple.getValue(1);
        ObjectId frank_id = new ObjectId();
        BasicDBList allxdrs = new BasicDBList();
        int cont = 0;

        try {

            MongoClient mongo = new MongoClient(new ServerAddress("localhost", 27317));
            final CreateCollectionOptions options = new CreateCollectionOptions();
            options.capped(true);
            options.sizeInBytes(10000);
            final MongoCollection coll = mongo.getDatabase("ministerio").getCollection("mo_call_xdrs_v2");
            int length = (int) coll.count();

            MongoCursor<Document> cursor = coll.find().iterator();
            if(cursor== null){
                LOG.info("No existen xdrs corralados por escenarios");
                this.collector.emit( new Values(" No existen datos corralados por escenarios! ", retInfo));

            }
            else {
                while (cursor.hasNext()) {

                    Document doc = cursor.next();
                    if (doc == null) break;


                    String userMSIDN = user.get("MSISDN").toString();
                    String xdrMSIDN;
                    if (doc.get("msisdn") == null) {
                        xdrMSIDN = "NULL";
                    } else {
                        xdrMSIDN = doc.get("msisdn").toString();
                    }

                    String userIMEI = user.get("IMEI").toString();
                    String xdrIMEI;
                    if (doc.get("imei") == null) {
                        xdrIMEI = "NULL";
                    } else {
                        xdrIMEI = doc.get("imei").toString();
                    }

                    String userIMSI = user.get("IMSI").toString();
                    String xdrIMSI;
                    if (doc.get("imsi") == null) {
                        xdrIMSI = "NULL";
                    } else {
                        xdrIMSI = doc.get("imsi").toString();
                    }

                /*
                System.out.println("FrankFilterUserDRPC _bolt: ************  User MSISDN: "+userMSIDN+" xdr MSISDN: "+xdrMSIDN+" **** ");
                System.out.println("FrankFilterUserDRPC _bolt: ************  User IMEI: "+userIMEI+" xdr IMEI: "+xdrIMEI+" **** ");
                System.out.println("FrankFilterUserDRPC _bolt: ************  User IMSI: "+userIMSI+" xdr IMSI: "+xdrIMSI+" **** ");
                */
                    if (/*true*/   userMSIDN.equals(xdrMSIDN) || userIMEI.equals(xdrIMEI) || userIMSI.equals(xdrIMSI)) {
                        ObjectId obj2 = (ObjectId) doc.get("_id");
                        LOG.info("FrankFilterUserDRPC _bolt: ************ Entra User " + user.get("TOKEN").toString() + " MSISDN: " + userMSIDN + " xdr MSISDN: " + xdrMSIDN + " **** ");
                        LOG.info("FrankFilterUserDRPC _bolt: ************  User IMEI: " + userIMEI + " xdr IMEI: " + xdrIMEI + " **** ");
                        LOG.info("FrankFilterUserDRPC _bolt: ************  User IMSI: " + userIMSI + " xdr IMSI: " + xdrIMSI + " **** ");
                        BasicDBObject document = new  BasicDBObject("id_copy_frank", obj2)
                                .append("msisdn", doc.get("msisdn"))
                                .append("imei", doc.get("imei"))
                                .append("imsi", doc.get("imsi"))
                                .append("xdr_raw", doc.get("xdr_raw"))
                                .append("end_time", doc.get("end_time") /*xdr.setDate((String) doc.get("end_time"), format)*/)
                                .append("start_time", doc.get("start_time") /*xdr.setDate((String) doc.get("start_time"), format)*/)
                                .append("token", user.get("TOKEN"))
                                .append("mandatory", doc.get("mandatory"));
                        //ObjectId obj = new ObjectId();
                        if (cont < length) {
                            allxdrs.add(document);
                            cont++;
                        }
                    } else {

                        LOG.info("FrankFilterUserDRPC _bolt: ************  No pertenece a este  usuario: **** " + user.get("TOKEN").toString());
                        LOG.info("FrankFilterUserDRPC _bolt: ************  User MSISDN: " + userMSIDN + " xdr MSISDN: " + xdrMSIDN + " **** ");
                        LOG.info("FrankFilterUserDRPC _bolt: ************  User IMEI: " + userIMEI + " xdr IMEI: " + xdrIMEI + " **** ");
                        LOG.info("FrankFilterUserDRPC _bolt: ************  User IMSI: " + userIMSI + " xdr IMSI: " + xdrIMSI + " **** ");
                    }

                }
                LOG.info(" Envio los datos corrralados al ultrabolt");
                this.collector.emit( TopologiesIDs.FrankStreamNames.FRANK_FILTER_USER_ALGORITM_STREAM, tuple, new Values( JSON.serialize(allxdrs), user.get("TOKEN"), retInfo));
            }

        }catch (Exception e) {
        //captura exception
    }




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream( TopologiesIDs.FrankStreamNames.FRANK_FILTER_USER_ALGORITM_STREAM, new Fields("xdrs", "token", "return-info"));
        outputFieldsDeclarer.declare(new Fields("result", "return-info"));

    }
}
