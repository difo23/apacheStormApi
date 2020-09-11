package com.ayscom.minetur.FrankXDRReload.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.ayscom.minetur.config.Config;
import com.ayscom.minetur.utils.MongoDBConnector;
import com.ayscom.minetur.utils.ServerConnection;
import com.mongodb.client.MongoCursor;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ayscom.minetur.config.TopologiesIDs;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankUserSpout extends BaseRichSpout {


    private static final Logger LOG = LoggerFactory.getLogger(FrankUserSpout.class);
    private SpoutOutputCollector _collector;
    private Map<Long, Document> _spoutCache;
    private Map<Document, Boolean> _spoutLock;
    private Boolean _ackEnabled = false;
    // LockEnabled : true significa cerrado por procesamiento, false libre.
    private Boolean _lockEnabled = true;
    private long _lastTransactionId = 0;
    private MongoDBConnector _mongodb;
    private MongoCursor<Document> _cursor;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream( TopologiesIDs.FrankStreamNames.FRANK_USER_SPOUT_TO_SCENARY_SELECT, new Fields("_user"));

    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
        _ackEnabled = true;
        _lastTransactionId = 0;
        _spoutCache = new HashMap<>();
        _spoutLock= new HashMap<>();
        _mongodb = MongoDBConnector.getConnector(new ServerConnection(Config.Database.USERS_SERVER_IP,
                Config.Database.USERS_SERVER_PORT));

    }


    @Override
    public void nextTuple() {


        if ((_cursor != null) && (_cursor.hasNext())) {

            Document user = _cursor.next();

            _lastTransactionId++;
            _spoutCache.put(_lastTransactionId, user);

            if(_spoutLock.containsKey(user)) {

                if(_spoutLock.get(user)){
                    LOG.info("Este usuario aun esta siendo procesado: "+user.toJson());
                }else {
                    LOG.info("Este usuario esta libre: "+user.toJson());
                    _spoutLock.replace(user, _lockEnabled);
                    LOG.info("User: "+user.toJson()+" lasttransactionid: "+_lastTransactionId);
                    _collector.emit(TopologiesIDs.FrankStreamNames.FRANK_USER_SPOUT_TO_SCENARY_SELECT, new Values(JSON.serialize(user)), _lastTransactionId);
                }

            }else{
                LOG.info("Este usuario es nuevo: "+user.toJson());
                _spoutLock.put(user, _lockEnabled);
                LOG.info("User: "+user.toJson()+" lasttransactionid: "+_lastTransactionId);
                _collector.emit(TopologiesIDs.FrankStreamNames.FRANK_USER_SPOUT_TO_SCENARY_SELECT, new Values(JSON.serialize(user)), _lastTransactionId);
            }

            LOG.info("User emitted [" + _lastTransactionId + "]");
            Utils.sleep(3000);

        } else {
            Utils.sleep(50000);
            _cursor = _mongodb.findAll(Config.Database.USERS_DATABASE, Config.Database.USERS_COLLECTION);
            //Si los da, volver a conectar
            //Si no hay datos nuevos, esperamos 50 segundos
            if (!_cursor.hasNext())
                Utils.sleep(50000);
        }
    }

    @Override
    public void ack(Object msgId) {
        long transactionId = (long) msgId;

        if (_ackEnabled) {
            _spoutCache.remove(transactionId);
            // con el transactionId busco el usuario que corresponde y remplazo su estado lockEnabled de true a false
            _spoutLock.replace(_spoutCache.get(transactionId), !_lockEnabled);
        }
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

    private void emitXDR(Document user, long transactionId) {

                _collector.emit(TopologiesIDs.FrankStreamNames.FRANK_USER_SPOUT_TO_SCENARY_SELECT, new Values(JSON.serialize(user)), transactionId);

        }


}