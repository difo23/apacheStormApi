package com.ayscom.minetur.frankDRPCRandom.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

/**
 * Created by lizandro on 2/08/15.
 */
public class FrankSaveGroupTimeDRPC extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankSaveGroupTimeDRPC.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {

        MongoClient mongo = new MongoClient(new ServerAddress("localhost", 27317));
        final CreateCollectionOptions options = new CreateCollectionOptions();
        options.capped(true);
        options.sizeInBytes(10000);

        try {

            final MongoCollection coll = mongo.getDatabase("ministerio").getCollection("xdrs_group");

            if (tuple.getSourceStreamId().equals("SaveGroup")) {

                ObjectId id_ini = (ObjectId) tuple.getValue(2);
                String token = tuple.getString(1);
                String input = tuple.getString(0);
                Document grupo = new Document();

                BasicDBList corraladospm = (BasicDBList) JSON.parse(input);

                /*Esto se puede modificar para que se envie de la clase anterior ya que es un metodo repetido*/
                DBObject dbObt;
                DBObject dbObt2 = (DBObject) (corraladospm.get(0));
                int count = 0;
                Date start = (Date) dbObt2.get("start_time");
                Date end =(Date) dbObt2.get("end_time");
                //token= dbObt2.get("token").toString();

                while (count < corraladospm.size()) {
                    dbObt = (DBObject) (corraladospm.get(count));
                    Date date_start = (Date) dbObt.get("start_time");
                    Date date_end = (Date) dbObt.get("end_time");

                    if (start.compareTo(date_start) == 1) {
                        start = date_start;
                    }
                    if (end.compareTo(date_end) == -1) {
                        end = date_end;
                    }

                    ++count;
                }


                LOG.info("Antes de guarda en db test collection grupos");
                grupo.append("id_frankDRPC", id_ini);
                grupo.append("xdrs_corralados", corraladospm.size());
                grupo.append("grupo_corralado", JSON.parse(input) );
                grupo.append("end_time_gp", end );
                grupo.append("start_time_gp", start);
                grupo.append("token", token);
                coll.insertOne(grupo);
                LOG.info(" ++++++++ Listo para guardar grupo corralado: User: " + token + " Xdrs corralados: " + input + " Count:" + count);
                ++count;


            }else{


            }


        }catch (IllegalArgumentException e) {
            // Hacer algo con la Exception

        }
    }

    //Declare that emitted tuples will contain a word field
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /// Este bolt no emite nada...
    }
}

