package com.ayscom.minetur.scenarios.bolts;

import backtype.storm.topology.base.BaseRichBolt;
import com.ayscom.minetur.XDR;
import com.ayscom.minetur.config.Config;
import com.ayscom.minetur.utils.MongoDBConnector;
import com.ayscom.minetur.utils.ServerConnection;

/**
 * Created by Christian on 10/7/15.
 */
public abstract class ScenariosBolt extends BaseRichBolt {
    private MongoDBConnector _mongodb;

    public void prepare() {
        _mongodb = MongoDBConnector.getConnector(new ServerConnection(Config.Database.RICH_XDR_SERVER_IP,
                Config.Database.RICH_XDR_SERVER_PORT));
    }

    public void insert_mo_call_xdr(XDR xdr) {
        insert_xdr(Config.Database.MO_CALL_XDR_DATABASE, Config.Database.MO_CALL_XDR_COLLECTION, xdr);
    }

    public void insert_xdr(String db_name, String collection_name, XDR xdr) {
        _mongodb.insertOrUpdate(db_name, collection_name, xdr.toBSON());
    }
}
