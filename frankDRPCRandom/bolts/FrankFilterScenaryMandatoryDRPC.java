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
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by lramirez on 6/08/15.
 */
public class FrankFilterScenaryMandatoryDRPC extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankFilterScenaryMandatoryDRPC.class);
    private OutputCollector collector;
    ObjectId id_frank;
    private Tuple tuple;
    private String token;
    private String input;
    private  Object retInfo;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector= outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        this.tuple=tuple;
        this.id_frank = (ObjectId) tuple.getValue(2);
        this.token = tuple.getString(1);
        this.input = tuple.getString(0);
        int count = 0;
        Boolean mandatory =  false;
        BasicDBList corralados = (BasicDBList) JSON.parse(input);

        while(count < corralados.size()){
            DBObject xdr = (DBObject) (corralados.get(count));
            if( (boolean) xdr.get("mandatory")){
                mandatory = true;
            }
            ++count;
        }

        if(mandatory){
            //Guardar en base de datos como un scenario del tipo que indica su campo scenary
            this.collector.emit(TopologiesIDs.FrankStreamNames.SCENARY_GROUP_SAVE_SCENARY_STREAM, new Values( JSON.serialize(corralados) ,this.token, this.id_frank));

        }else{
            // Guardar como un grupo corralado por tiempo sin escenario definido
            this.collector.emit(TopologiesIDs.FrankStreamNames.SCENARY_GROUP_SAVE_GROUP_STREAM, new Values( JSON.serialize(corralados) ,this.token, this.id_frank));

        }



    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream( TopologiesIDs.FrankStreamNames.SCENARY_GROUP_SAVE_SCENARY_STREAM, new Fields("xdrs", "token", "return-info"));
        outputFieldsDeclarer.declareStream( TopologiesIDs.FrankStreamNames.SCENARY_GROUP_SAVE_GROUP_STREAM, new Fields("xdrs", "token", "return-info"));
    }

}
