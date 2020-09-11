package com.ayscom.minetur.FrankXDRReload.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.ayscom.minetur.FrankXDRReload.FrankUtilities.FrankUtils;
import com.ayscom.minetur.config.Config;
import com.ayscom.minetur.config.TopologiesIDs;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Random;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankRecursiveBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankRecursiveBolt.class);
    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector= outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Inicio execute: ");
        String _input = tuple.getString(0);
        String result="...";
        String scenarytype = tuple.getString(1);
        BasicDBList xdrs = (BasicDBList) JSON.parse(_input);
        LOG.info(" Input: "+_input );
        LOG.info(" Scenarytype: "+_input );
        LOG.info("Inicia frank recursivo");
        if(xdrs.size()>0) {
            result = frankestain(xdrs, scenarytype, tuple);
        }
        LOG.info(" ID peticion : "+_input+"  resultado: "+result+" scenarytype:"+scenarytype);
        _collector.ack(tuple);
    }


    public String frankestain(BasicDBList xdrs, String scenarytype, Tuple tuple) {
        BasicDBList allcorraladosim= new BasicDBList();

        BasicDBList result = frankestainRecurive(xdrs, allcorraladosim);
        if (result !=null) {
            _collector.emit(TopologiesIDs.FrankStreamNames.FRANK_RECURSIVE_ALGORTIM_TO_SCENARY_MADATORY, tuple, new Values( JSON.serialize(result), scenarytype));
            return  " Estan todos corralados!";
        } else {
            //this._collector.emit(new Values("Intentalo mas tarde!", _retInfo));
            return  " Existe un error no es posible corralar! ";
        }
    }



    public BasicDBList  frankestainRecurive(BasicDBList xdrs, BasicDBList allcorr){

        int index;
        int rango;
       // Random rdn= new Random();
        BasicDBList xdrs_group= new BasicDBList();
        ArrayList<BasicDBList> result;
        BasicDBList allcorrelados = allcorr;

        if(xdrs != null) {
            if (xdrs.size() == 0){
                return allcorrelados;

            }else {
                rango = xdrs.size()-1;
                index = rango /*rdn.nextInt(rango)*/;
                LOG.info("El index random generado es: " + index);
                xdrs_group.add(xdrs.remove(index));
                result = frankIMRandom(xdrs, xdrs_group);

                if(result.get(1).size()>=1) {
                    allcorrelados.add(result.get(1));
                }

                if (result.get(0).size() == 0) {
                    return allcorrelados;
                } else {
                    frankestainRecurive(result.get(0), allcorrelados);
                }
            }

        }else{
            return null;
        }
       return allcorrelados;
    }



    public   ArrayList<BasicDBList> frankIMRandom( BasicDBList nocorralados, BasicDBList corralados ) {

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
            xdrs_im = new BasicDBList();
            xdrs_no_im = new BasicDBList();

            LOG.info("Llamada a setInterval");
            BasicDBObject time = FrankUtils.setInterval(corraladosim);
            Date  start = (Date) time.get("start");
            Date end = (Date) time.get("end");

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
        ArrayList<BasicDBList> result=new ArrayList<>();
        result.add(0,nocorraladosim);
        result.add(1,corraladosim);

        return result;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_RECURSIVE_ALGORTIM_TO_SCENARY_MADATORY, new Fields("allgruposim","scenarytype"));
    }


}