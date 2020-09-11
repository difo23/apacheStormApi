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

import java.util.Date;
import java.util.Map;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankScenaryMandatoryBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FrankScenaryMandatoryBolt.class);
    private OutputCollector _collector;




    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info(" Inicio execute: ");
        String _scenarytype = tuple.getString(1);
        String _input = tuple.getString(0);

        LOG.info(" Input: "+_input );
        LOG.info(" scenarytype: "+_scenarytype );


        BasicDBList _mandatoryuserxdrs= new BasicDBList();
        BasicDBList _mandatoryuserrefusexdrs= new BasicDBList();

        Object _msisdn;
        Object _imei;
        Object _imsi;

        Boolean mandatory;
        Boolean user;


        BasicDBList xdrimlist = (BasicDBList) JSON.parse(_input);

        if(xdrimlist.size() > 0) {
            for(Object xdrim: xdrimlist){

                mandatory =  false;
                user = false;

                _msisdn = new Object();
                _imei = new Object();
                _imsi = new Object();

                BasicDBList corralados = (BasicDBList) xdrim;

                for(Object corr: corralados){
                    BasicDBObject xdr = (BasicDBObject) corr;
                    if ((boolean) xdr.get("mandatory")) {
                        mandatory = true;
                    }

                    if (xdr.get("msisdn") != null && xdr.get("imei") != null && xdr.get("imsi") != null) {
                        user = true;
                        _msisdn = xdr.get("msisdn");
                        _imei = xdr.get("imei");
                        _imsi = xdr.get("imsi");
                    }

                }

                LOG.info("Llamada a setInterval");
                BasicDBObject time = FrankUtils.setInterval(corralados);
                Date start = (Date) time.get("start");
                Date end = (Date) time.get("end");

                BasicDBObject xdrgroup = new BasicDBObject();
                xdrgroup.append("msisdn", _msisdn);
                xdrgroup.append("imei", _imei);
                xdrgroup.append("imsi", _imsi);
                xdrgroup.append("start_time", start);
                xdrgroup.append("end_time", end);
                xdrgroup.append("xdrs_correlados", corralados.size());
                xdrgroup.append("scenary_type", _scenarytype);
                xdrgroup.append("xdrs", corralados);

                if (mandatory && user) {

                    _mandatoryuserxdrs.add(xdrgroup);

                } else {
                    xdrgroup.append("condition_mandatory", mandatory);
                    xdrgroup.append("condition_user", user);
                    _mandatoryuserrefusexdrs.add(xdrgroup);
                }

            }
            if( _mandatoryuserxdrs.size()>0) {
                this._collector.emit(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_MANDATORY_TO_LOGS_USER, tuple, new Values(JSON.serialize( _mandatoryuserxdrs), _scenarytype));
            }else{
                LOG.info("No cumplen condicion de mandatorio y de usuario unico : "+_mandatoryuserrefusexdrs.toString());
                this._collector.emit(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_MANDATORY_TO_REFUSE_GROUPS, tuple,new Values(JSON.serialize(  _mandatoryuserrefusexdrs)));
            }

            _collector.ack(tuple);

        }else{
            _collector.fail(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_MANDATORY_TO_LOGS_USER, new Fields("grupos", "scenarytype"));
        outputFieldsDeclarer.declareStream(TopologiesIDs.FrankStreamNames.FRANK_SCENARY_MANDATORY_TO_REFUSE_GROUPS, new Fields("grupos" ));
    }
}
