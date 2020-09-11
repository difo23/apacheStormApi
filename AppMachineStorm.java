package com.ayscom.minetur;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.ayscom.minetur.config.Protocols;
import com.ayscom.minetur.config.TopologiesIDs;
import com.ayscom.minetur.frankDRPCRandom.bolts.FrankAlgoritmTimeGroupDRPC;
import com.ayscom.minetur.frankDRPCRandom.bolts.FrankFilterUserDRPC;
import com.ayscom.minetur.frankDRPCRandom.bolts.FrankSaveGroupTimeDRPC;
import com.ayscom.minetur.scenarios.bolts.IuCs.AInterface.IuCsAInterfaceCallBolt;
import com.ayscom.minetur.scenarios.bolts.IuCs.AInterface.IuCsAInterfaceCallSetupBolt;
import com.ayscom.minetur.scenarios.bolts.IuCs.IuCsMessageBolt;
import com.ayscom.minetur.scenarios.bolts.IuCs.IuCsOtherBolt;
import com.ayscom.minetur.scenarios.bolts.IuCs.IuCsRanBolt;
import com.ayscom.minetur.scenarios.bolts.isup.IsupCallBolt;
import com.ayscom.minetur.scenarios.bolts.suap.S1APOtherBolt;
import com.ayscom.minetur.scenarios.bolts.suap.S1APPagingBolt;
import com.ayscom.minetur.scenarios.spouts.CrudeXDRSpout;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static backtype.storm.utils.Utils.tuple;

/**
 * All topologys real distributed Storm submiter
 *
 */
public class AppMachineStorm {

    private static final Logger LOG = LoggerFactory.getLogger(AppMachineStorm.class);
    private static final TopologyBuilder frankDRPCBuilder = new TopologyBuilder();
    private static final TopologyBuilder scenaryBuilder = new TopologyBuilder();
    private static final LocalDRPC drpc = new LocalDRPC();
    private static final LocalCluster cluster= new LocalCluster();;
    private static final Config conf = new Config();


    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {

        //If there are arguments, we are running on a cluster
        if (args != null && args.length > 0) {

            LOG.info("Entro en modo Remoto");
            //conf.setDebug(true);
            //parallelism hint to set the number of workers
            conf.setNumWorkers(3);// Cada worker representa una JVM, por default el numero de Worker es 1.
            buildTopologies(false);
            //submit the topology Frank DRPC
            StormSubmitter.submitTopologyWithProgressBar(args[0]+ TopologiesIDs.TopologiesNames.FRANK_DRPC_TOPOLOGY, conf, frankDRPCBuilder.createTopology());

             conf.setNumWorkers(3);
            // Envio topologia Scenary
            // StormSubmitter.submitTopologyWithProgressBar(args[0] + TopologiesIDs.TopologiesNames.SCENARY_TOPOLOGY, conf, scenaryBuilder.createTopology());

        }
        //Otherwise, we are running locally
        else {

            LOG.info("Entro en modo test Local");
            //Cap the maximum number of executors that can be spawned
            conf.setDebug(true);
            //for a component to 3
            conf.setMaxTaskParallelism(3);

            //LocalCluster is used to run locally
            buildTopologies(true);

            //submit the topologies
            cluster.submitTopology(TopologiesIDs.TopologiesNames.FRANK_DRPC_TOPOLOGY, conf,  frankDRPCBuilder.createTopology());
          //  cluster.submitTopology(TopologiesIDs.TopologiesNames.SCENARY_TOPOLOGY, conf, scenaryBuilder.createTopology());

            //Test DRPC CLient Local
            testDRPCClientLocal();

            //sleep
            Thread.sleep(10000);
            //shut down the cluster
            cluster.shutdown();
            drpc.shutdown();
        }
    }


    private static void  buildTopologies(Boolean drpcLocal){

        DRPCSpout frankDRPC;
                if(drpcLocal){
                    frankDRPC= new DRPCSpout(TopologiesIDs.TopologiesNames.FRANK_DRPC_TOPOLOGY, drpc);
                }else{
                    frankDRPC= new DRPCSpout(TopologiesIDs.TopologiesNames.FRANK_DRPC_TOPOLOGY);
                }
        //Configurando el numero de Excecutors (Threads) y task (bolts/spouts instances metodos execute()/nextTuple())
        frankDRPCBuilder.setSpout(TopologiesIDs.FrankSpoutsIDs.SPOUT_DRPC, frankDRPC, 3);
        frankDRPCBuilder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_FILTER_USER_DRPC, new FrankFilterUserDRPC(), 3).setNumTasks(6).shuffleGrouping(TopologiesIDs.FrankSpoutsIDs.SPOUT_DRPC);
        frankDRPCBuilder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_ALGORITM_TIME_GROUP_DRPC, new FrankAlgoritmTimeGroupDRPC(), 6).shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_FILTER_USER_DRPC, TopologiesIDs.FrankStreamNames.FRANK_FILTER_USER_ALGORITM_STREAM);
        frankDRPCBuilder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SAVE_GROUP_TIME_DRPC, new FrankSaveGroupTimeDRPC(), 3).shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_ALGORITM_TIME_GROUP_DRPC, TopologiesIDs.FrankStreamNames.SCENARY_GROUP_SAVE_GROUP_STREAM);
        frankDRPCBuilder.setBolt(TopologiesIDs.FrankBoltsIDs.RETURN, new ReturnResults(), 3).shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_ALGORITM_TIME_GROUP_DRPC).shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_FILTER_USER_DRPC);

        scenaryBuilder.setSpout(TopologiesIDs.ScenSpoutsIDS.CRUDE_XDR_SPOUT, new CrudeXDRSpout());
        scenaryBuilder.setBolt(TopologiesIDs.ScenBoltsIDS.IU_CS_AINTERFACE_CALL_BOLT, new IuCsAInterfaceCallBolt(), 4).shuffleGrouping(TopologiesIDs.ScenSpoutsIDS.CRUDE_XDR_SPOUT, Protocols.IuCs.AInterface.Call);
        scenaryBuilder.setBolt(TopologiesIDs.ScenBoltsIDS.IU_CS_AINTERFACE_CALL_SETUP_BOLT, new IuCsAInterfaceCallSetupBolt(), 4).shuffleGrouping(TopologiesIDs.ScenSpoutsIDS.CRUDE_XDR_SPOUT, Protocols.IuCs.AInterface.CallSetup);
        scenaryBuilder.setBolt(TopologiesIDs.ScenBoltsIDS.IU_CS_MESSAGE_BOLT,  new IuCsMessageBolt(), 4).shuffleGrouping(TopologiesIDs.ScenSpoutsIDS.CRUDE_XDR_SPOUT, Protocols.IuCs.Message);
        scenaryBuilder.setBolt(TopologiesIDs.ScenBoltsIDS.IU_CS_RAN_BOLT, new IuCsRanBolt(), 4).shuffleGrouping(TopologiesIDs.ScenSpoutsIDS.CRUDE_XDR_SPOUT, Protocols.IuCs.RAN);
        scenaryBuilder.setBolt(TopologiesIDs.ScenBoltsIDS.IU_CS_OTHER_BOLT, new IuCsOtherBolt(), 4).shuffleGrouping(TopologiesIDs.ScenSpoutsIDS.CRUDE_XDR_SPOUT, Protocols.IuCs.Other);
        scenaryBuilder.setBolt(TopologiesIDs.ScenBoltsIDS.SIAP_OTHER_BOLT, new S1APOtherBolt(), 4).shuffleGrouping(TopologiesIDs.ScenSpoutsIDS.CRUDE_XDR_SPOUT, Protocols.S1AP.Other);
        scenaryBuilder.setBolt(TopologiesIDs.ScenBoltsIDS.S1AP_PAGING_BOLT, new S1APPagingBolt(), 4).shuffleGrouping(TopologiesIDs.ScenSpoutsIDS.CRUDE_XDR_SPOUT, Protocols.S1AP.Paging);
        scenaryBuilder.setBolt(TopologiesIDs.ScenBoltsIDS.ISUP_CALL_BOLT, new IsupCallBolt(), 4).shuffleGrouping(TopologiesIDs.ScenSpoutsIDS.CRUDE_XDR_SPOUT, Protocols.ISUP.Call);

    }


    private static void  testDRPCClientLocal(){
        LOG.info("Test DRPC function in localCluster");
        Document doc_user = new Document();
        LOG.info("Test topology Cliente Java");
        ObjectId id = new ObjectId();
        doc_user.append("id", id);
        doc_user.append("data" , 3);
        doc_user.append("MSISDN", "34633998875" );
        doc_user.append("IMEI", "355828063300862");
        doc_user.append("IMSI", "214040101801031");
        doc_user.append("SCENARY", "MO");
        LOG.info("Test topology Cliente Java: "+doc_user.toJson());

        LOG.info("Results for DRPC debug:" + drpc.execute(TopologiesIDs.TopologiesNames.FRANK_DRPC_TOPOLOGY, JSON.serialize(doc_user)));

    }

}
