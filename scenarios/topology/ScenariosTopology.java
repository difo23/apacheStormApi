package com.ayscom.minetur.scenarios.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.ayscom.minetur.config.Protocols;
import com.ayscom.minetur.scenarios.bolts.IuCs.AInterface.IuCsAInterfaceCallBolt;
import com.ayscom.minetur.scenarios.bolts.IuCs.AInterface.IuCsAInterfaceCallSetupBolt;
import com.ayscom.minetur.scenarios.bolts.IuCs.IuCsMessageBolt;
import com.ayscom.minetur.scenarios.bolts.IuCs.IuCsOtherBolt;
import com.ayscom.minetur.scenarios.bolts.IuCs.IuCsRanBolt;
import com.ayscom.minetur.scenarios.bolts.isup.IsupCallBolt;
import com.ayscom.minetur.scenarios.bolts.suap.S1APOtherBolt;
import com.ayscom.minetur.scenarios.bolts.suap.S1APPagingBolt;
import com.ayscom.minetur.scenarios.spouts.CrudeXDRSpout;

/**
 * Created by Christian on 29/6/15.
 */
public class ScenariosTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        CrudeXDRSpout crudeXDRSpout = new CrudeXDRSpout();
        IuCsAInterfaceCallBolt iuCsAInterfaceCallBolt = new IuCsAInterfaceCallBolt();
        IuCsAInterfaceCallSetupBolt iuCsAInterfaceCallSetupBolt = new IuCsAInterfaceCallSetupBolt();
        IuCsMessageBolt iuCsMessageBolt = new IuCsMessageBolt();
        IuCsRanBolt iuCsRanBolt = new IuCsRanBolt();
        IuCsOtherBolt iuCsOtherBolt = new IuCsOtherBolt();
        S1APOtherBolt s1apOtherBolt = new S1APOtherBolt();
        S1APPagingBolt s1apPagingBolt = new S1APPagingBolt();
        IsupCallBolt isupCallBolt = new IsupCallBolt();

        topologyBuilder.setSpout("crude_xdr_spout", crudeXDRSpout);

        topologyBuilder.setBolt("iu_cs_ainterface_call_bolt", iuCsAInterfaceCallBolt, 4).shuffleGrouping("crude_xdr_spout", Protocols.IuCs.AInterface.Call);
        topologyBuilder.setBolt("iu_cs_ainterface_call_setup_bolt", iuCsAInterfaceCallSetupBolt, 4).shuffleGrouping("crude_xdr_spout", Protocols.IuCs.AInterface.CallSetup);
        topologyBuilder.setBolt("iu_cs_message_bolt", iuCsMessageBolt, 4).shuffleGrouping("crude_xdr_spout", Protocols.IuCs.Message);
        topologyBuilder.setBolt("iu_cs_ran_bolt", iuCsRanBolt, 4).shuffleGrouping("crude_xdr_spout", Protocols.IuCs.RAN);
        topologyBuilder.setBolt("iu_cs_other_bolt", iuCsOtherBolt, 4).shuffleGrouping("crude_xdr_spout", Protocols.IuCs.Other);
        topologyBuilder.setBolt("s1ap_other_bolt", s1apOtherBolt, 4).shuffleGrouping("crude_xdr_spout", Protocols.S1AP.Other);
        topologyBuilder.setBolt("s1ap_paging_bolt", s1apPagingBolt, 4).shuffleGrouping("crude_xdr_spout", Protocols.S1AP.Paging);
        topologyBuilder.setBolt("isup_call_bolt", isupCallBolt, 4).shuffleGrouping("crude_xdr_spout", Protocols.ISUP.Call);

        Config conf = new Config();
        conf.setDebug(true);

        //If there are arguments, we are running on a cluster
        if (args != null && args.length > 0) {
            //parallelism hint to set the number of workers
            conf.setNumWorkers(3);
            //submit the topology
            StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());

        }
        //Otherwise, we are running locally
        else {
            //Cap the maximum number of executors that can be spawned
            //for a component to 3
            conf.setMaxTaskParallelism(3);
            //LocalCluster is used to run locally
            LocalCluster cluster = new LocalCluster();
            //submit the topology
            cluster.submitTopology("minetur_storm", conf, topologyBuilder.createTopology());


        }
    }

}
