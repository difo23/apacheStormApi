package com.ayscom.minetur.scenarios.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.ayscom.minetur.config.Config;
import com.ayscom.minetur.config.Protocols;
import com.ayscom.minetur.utils.MongoDBConnector;
import com.ayscom.minetur.utils.ServerConnection;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Christian on 25/6/15.
 */
public class CrudeXDRSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private Map<Long, Document> _spoutCache;
    private Boolean _ackEnabled = false;
    private long _lastTransactionId = 0;
    private MongoDBConnector _mongodb;
    private MongoCursor<Document> _cursor;

    static Logger LOG = Logger.getLogger(CrudeXDRSpout.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(Protocols.IuCs.AInterface.Call, new Fields("_xdr"));
        outputFieldsDeclarer.declareStream(Protocols.IuCs.AInterface.CallSetup, new Fields("_xdr"));
        outputFieldsDeclarer.declareStream(Protocols.IuCs.Message, new Fields("_xdr"));
        outputFieldsDeclarer.declareStream(Protocols.IuCs.RAN, new Fields("_xdr"));
        outputFieldsDeclarer.declareStream(Protocols.IuCs.Other, new Fields("_xdr"));
        outputFieldsDeclarer.declareStream(Protocols.ISUP.Call, new Fields("_xdr"));
        outputFieldsDeclarer.declareStream(Protocols.Map.Transaction, new Fields("_xdr"));
        outputFieldsDeclarer.declareStream(Protocols.S1AP.Other, new Fields("_xdr"));
        outputFieldsDeclarer.declareStream(Protocols.S1AP.Paging, new Fields("_xdr"));

    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        _ackEnabled = true;
        _lastTransactionId = 0;
        _spoutCache = new HashMap<>();
        _mongodb = MongoDBConnector.getConnector(new ServerConnection(Config.Database.CRUDE_XDR_SERVER_IP,
                Config.Database.CRUDE_XDR_SERVER_PORT));
        _cursor = _mongodb.findAll(Config.Database.CRUDE_XDR_DATABASE, Config.Database.CRUDE_XDR_COLLECTION);
    }

    @Override
    public void nextTuple() {
        //Si el cursor de la base de datos es nulo o a llegado al final
            //Se hace una nueva query para traer mas datos
        //Emite el siguiente elemento en la lista del cursor
        if ((_cursor != null) && (_cursor.hasNext())) {
            Document xdr = _cursor.next();
            _lastTransactionId++;
            _spoutCache.put(_lastTransactionId, xdr);
            LOG.info("XDR emitted [" + _lastTransactionId + "]");
            emitXDR(xdr, _lastTransactionId);
        } else {
            //_cursor = _mongodb.findAll(Config.Database.CRUDE_XDR_COLLECTION); //Comprobar si da errores por perdida de conexion
                                                        //Si los da, volver a conectar
            //Si no hay datos nuevos, esperamos 50 segundos
            if (!_cursor.hasNext())
                Utils.sleep(50000);
        }
    }

    @Override
    public void ack(Object msgId) {
        long transactionId = (long) msgId;

        if (_ackEnabled)
            _spoutCache.remove(transactionId);

        LOG.info("Message fully processed [" + transactionId + "]");
    }

    @Override
    public void fail(Object msgId) {
        long transactionId = (long) msgId;

        if (_ackEnabled) {
            if (!_spoutCache.containsKey(transactionId))
                throw new RuntimeException("Error, transactionId not found [" + transactionId + "]");
            emitXDR(_spoutCache.get(transactionId), transactionId);
            LOG.info("Re-sending message [" + transactionId + "]");
        }
    }

    private void emitXDR(Document xdr, long transactionId) {
        String protocol = xdr.getString("Protocol");
        _collector.emit(protocol, new Values(xdr), transactionId);
    }
}
