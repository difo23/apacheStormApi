package com.ayscom.minetur.FrankXDRReload.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.ayscom.minetur.FrankXDRReload.bolts.*;
import com.ayscom.minetur.FrankXDRReload.spouts.FrankUserSpout;
import com.ayscom.minetur.config.TopologiesIDs;
import org.slf4j.Logger;
import com.ayscom.minetur.config.TopologiesIDs.FrankStreamNames;
import org.slf4j.LoggerFactory;

/**
 * Created by lramirez on 10/08/15.
 */
public class FrankReloadTopology  {

    private static final Logger LOG = LoggerFactory.getLogger(FrankReloadTopology.class);

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout(TopologiesIDs.FrankSpoutsIDs.FRANK_SPOUT_USERS,new  FrankUserSpout(),1);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_SELECT_MO_BOLT,
                new FrankScenarySelectMOBolt(), 1).setNumTasks(1).shuffleGrouping(TopologiesIDs.FrankSpoutsIDs.FRANK_SPOUT_USERS,
                        FrankStreamNames.FRANK_USER_SPOUT_TO_SCENARY_SELECT);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_SELECT_MT_BOLT,
                new FrankScenarySelectMTBolt(), 1).setNumTasks(1).shuffleGrouping(TopologiesIDs.FrankSpoutsIDs.FRANK_SPOUT_USERS,
                        FrankStreamNames.FRANK_USER_SPOUT_TO_SCENARY_SELECT);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_SELECT_MO_SMS_BOLT,
                new FrankScenarySelectMOSMSBolt(), 1).setNumTasks(1).shuffleGrouping(TopologiesIDs.FrankSpoutsIDs.FRANK_SPOUT_USERS,
                        FrankStreamNames.FRANK_USER_SPOUT_TO_SCENARY_SELECT);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_SELECT_MT_SMS_BOLT,
                new FrankScenarySelectMTSMSBolt(), 1).setNumTasks(1).shuffleGrouping(TopologiesIDs.FrankSpoutsIDs.FRANK_SPOUT_USERS,
                        FrankStreamNames.FRANK_USER_SPOUT_TO_SCENARY_SELECT);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_SELECT_MOBILITY_BOLT,
                new FrankScenarySelectMobilityBolt(), 1).setNumTasks(1).shuffleGrouping(TopologiesIDs.FrankSpoutsIDs.FRANK_SPOUT_USERS,
                        FrankStreamNames.FRANK_USER_SPOUT_TO_SCENARY_SELECT);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_RECURSIVE_BOLT, new FrankRecursiveBolt(), 1)
               // .shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_SELECT_MO_BOLT,
                //        FrankStreamNames.FRANK_SCENARY_SELECT_MO_TO_RECURSIVE_ALGORITM)
                //.shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_SELECT_MT_BOLT,
                  //      FrankStreamNames.FRANK_SCENARY_SELECT_MT_TO_RECURSIVE_ALGORITM)
                .shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_SELECT_MO_SMS_BOLT,
                        FrankStreamNames.FRANK_SCENARY_SELECT_MO_SMS_TO_RECURSIVE_ALGORITM);
                //.shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_SELECT_MT_SMS_BOLT,
                  //      FrankStreamNames.FRANK_SCENARY_SELECT_MT_SMS_TO_RECURSIVE_ALGORITM)
                //.shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_SELECT_MOBILITY_BOLT,
                  //      FrankStreamNames.FRANK_SCENARY_SELECT_MOBILITY_TO_RECURSIVE_ALGORITM);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_MANDATORY_BOLT, new FrankScenaryMandatoryBolt(), 1)
                .shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_RECURSIVE_BOLT,
                        FrankStreamNames.FRANK_RECURSIVE_ALGORTIM_TO_SCENARY_MADATORY);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_LOGS_USER_BOLT, new FrankLogsUserBolt(), 1)
                .shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_MANDATORY_BOLT,
                        FrankStreamNames.FRANK_SCENARY_MANDATORY_TO_LOGS_USER);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_REFUSE_GROUPS_BOLT, new FrankSaveRefuseGroupsBolt(), 1)
                .shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_MANDATORY_BOLT,
                        FrankStreamNames.FRANK_SCENARY_MANDATORY_TO_REFUSE_GROUPS);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_FINAL_BOLT, new FrankScenaryFinalBolt(), 1).
                shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_LOGS_USER_BOLT,
                        FrankStreamNames.FRANK_LOGS_USER_TO_SCENARY_FINAL);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_TEMP_BOLT, new FrankScenaryTempBolt(), 1)
                .shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_LOGS_USER_BOLT,
                        FrankStreamNames.FRANK_LOGS_USER_TO_SCENARY_TEMP);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SAVE_GROUPS_TEMP_BOLT, new FrankSaveTempGroupsBolt(), 1)
                .shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_TEMP_BOLT,
                        FrankStreamNames.FRANK_SCENARY_TEMP_TO_SAVE_GROUP);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_DELETE_XDRS_BOLT, new FrankDeleteXDRSBolt(), 1)
                .shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_FINAL_BOLT,
                        FrankStreamNames.FRANK_SCENARY_FINAL_TO_DELETE_XDRS);

        builder.setBolt(TopologiesIDs.FrankBoltsIDs.FRANK_SAVE_GROUPS_FINAL_BOLT, new FrankSaveFinalGroupsBolt(), 1)
                .shuffleGrouping(TopologiesIDs.FrankBoltsIDs.FRANK_SCENARY_FINAL_BOLT,
                        FrankStreamNames.FRANK_SCENARY_FINAL_TO_SAVE_GROUP);


        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();


        // Configurando el numero de workers
        conf.setNumWorkers(2);// Cada worker representa una JVM, por default el numero de Worker es 1.
        //conf.setMaxTaskParallelism(3);// en modo local simula el numero de workers
        //cluster.submitTopology("FrankReloadTopology", conf, builder.createTopology());
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());

        //Thread.sleep(10000);
        //cluster.shutdown();
        //drpc.shutdown();

    }


}