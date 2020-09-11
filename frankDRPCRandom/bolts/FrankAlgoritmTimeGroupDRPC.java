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
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.Random;

/**
 * Created by lramirez on 4/08/15.
 */
public class FrankAlgoritmTimeGroupDRPC extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankAlgoritmTimeGroupDRPC.class);
    private OutputCollector collector;
    private Tuple tuple;
    private String token;
    private ObjectId id_frank;
    private  Object retInfo;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector= outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Inicio execute: ");
        this.tuple=tuple;
        this.id_frank = new ObjectId();

        this.retInfo = tuple.getValue(2);
        String token = tuple.getString(1);
        String input = tuple.getString(0);
        this.token=token;

        BasicDBList xdrs = (BasicDBList) JSON.parse(input);

        LOG.info(" RetInfo: "+retInfo);
        LOG.info(" Input: "+input );
        LOG.info("Emit para bolt ResultDRPC ");
        String result = frankestain(xdrs);
        LOG.info(" ID peticion : "+this.id_frank+" resultado: "+result);
    }

    public String frankestain(BasicDBList xdrs) {

        String result = frankestainRecurive(xdrs);
        if (result == "OK") {
            BasicDBObject id = new BasicDBObject("id_frankDRPC", this.id_frank);
            this.collector.emit(new Values(this.id_frank.toString(), retInfo));
            return result + " Estan todos corralados!";
        } else {
            this.collector.emit(new Values("Intentalo mas tarde!", retInfo));
            return result + " Existe un error no es posible corralar! ";
        }
    }



    public String frankestainRecurive(BasicDBList xdrs){

            int index;
            int rango;
            Random rdn= new Random();
            BasicDBList xdrs_group= new BasicDBList();
            BasicDBList result = new BasicDBList();

        if(xdrs != null) {
            if (xdrs.size() == 0){
                return "OK";

            }else {

                rango = xdrs.size()-1;
                index = rango /*rdn.nextInt(rango)*/;
                LOG.info("El index random generado es: " + index);
                xdrs_group.add(xdrs.remove(index));
                result = frankIMRandom(xdrs, xdrs_group);

                if (result.size() == 0) {
                    return "OK";
                } else {
                    frankestainRecurive(result);
                }
            }

        }else{
            return "ERR-xdrs = null";
        }
        return "OK";
    }



    public BasicDBObject setInterval( BasicDBList xdrs ) {

        LOG.info("Inicio setInterval:");
        LOG.info("XDRS para buscar el intevalo size:"+xdrs.size()+" contenido:"+xdrs.toString());

        BasicDBObject time = new BasicDBObject();
        Date end;
        Date start;
        DBObject dbObt;
        DBObject dbObt2 = (DBObject) (xdrs.get(0));
        int count = 0;


        start = (Date) dbObt2.get("start_time");
        end = (Date) dbObt2.get("end_time");

        LOG.info("Start time  para inicial la busqueda "+start.toString());
        LOG.info("End time  para inicial la busqueda "+end.toString());

        while (count < xdrs.size()) {
            LOG.info("Entro al loop");
            dbObt = (DBObject) (xdrs.get(count));
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
        LOG.info("Fuera del loop "+end.toString());
        time.put("end", end);
        time.put("start", start);
        LOG.info("carga a time");

        LOG.info("Finalizo setInterval:");
        LOG.info("Resultados start: "+time.get("start")+" end:"+time.get("end"));

        return time;
    }


    public BasicDBList frankIMRandom( BasicDBList nocorralados, BasicDBList corralados ) {

        BasicDBList corraladosim;
        BasicDBList nocorraladosim;
        BasicDBList xdrs_im;
        BasicDBList xdrs_no_im;


        LOG.info("Inicio frankIMRandom:");
        LOG.info("XDRS corralados size:"+corralados.size()+" contenido:"+corralados.toString());
        LOG.info("XDRS no corralados size:"+nocorralados.size()+" contenido:"+nocorralados.toString());
        corraladosim = corralados;
        nocorraladosim = nocorralados;

        do {
            DBObject dbObt;
            int count = 0;
            Date start;
            Date end;
            xdrs_im = new BasicDBList();
            xdrs_no_im = new BasicDBList();

            LOG.info("Llamada a setInterval");
            BasicDBObject time = setInterval( corraladosim );
            start = (Date) time.get("start");
            end = (Date) time.get("end");


            while (count < nocorraladosim.size()) {

                dbObt = (DBObject) (nocorraladosim.get(count));
                Date date_start =  (Date) dbObt.get("start_time");
                Date date_end = (Date) dbObt.get("end_time");

                if ((end.getTime() >= date_start.getTime()) && (date_start.getTime() >= start.getTime())) {
                    xdrs_im.add(dbObt);
                    LOG.info(" ************ Entra en intervalo medio:" + dbObt.toString());

                } else if ((start.getTime() <= date_end.getTime()) && (date_end.getTime() <= end.getTime())) {
                    xdrs_im.add(dbObt);
                    LOG.info(" ************ Entra en intervalo medio:" + dbObt.toString());
                } else {
                    xdrs_no_im.add(dbObt);
                    LOG.info(" ************ NO! Entra en intervalo medio:" + dbObt.toString());
                }
                ++count;
            }


            if (xdrs_im.size() > 0) {
                int cont = 0;
                while (cont < xdrs_im.size()) {
                    LOG.info(" ***El grupo corralado por random crece constantemente al incrementar el numero de xdrs corralado por IM.");
                    corraladosim.add(corraladosim.size(), xdrs_im.get(cont));
                    ++cont;
                }
            }

            nocorraladosim = xdrs_no_im;

            LOG.info(" ************ Repuesta de metodo corraladosim: **** " + corraladosim.toString() + " size:" + corraladosim.size());
            LOG.info("  ************ Repuesta de metodo nocorraladosim: **** " + nocorraladosim.toString() + " size:" + nocorraladosim.size());

        }while (xdrs_im.size() > 0);


        this.collector.emit(TopologiesIDs.FrankStreamNames.FRANK_ALGORITM_SCENARY_GROUP_STREAM, new Values( JSON.serialize(corraladosim) ,this.token, this.id_frank));
        return nocorraladosim;
    }




    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields("result","return-info"));
        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_ALGORITM_SCENARY_GROUP_STREAM, new Fields("grupos","token", "idfrank"));
    }


}
