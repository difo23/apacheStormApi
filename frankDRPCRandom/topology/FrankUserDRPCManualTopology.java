package com.ayscom.minetur.frankDRPCRandom.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.ayscom.minetur.frankDRPCRandom.bolts.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by lizandro on 2/08/15.
 */
public class FrankUserDRPCManualTopology {

    private static final Logger LOG = LoggerFactory.getLogger(FrankUserDRPCManualTopology.class);

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        /*  Para depuracion se recomienda usar LocalDRPC un ejemplo:
            LocalDRPC drpc = new LocalDRPC();
            DRPCSpout spout = new DRPCSpout("FrankManualDRPC", drpc);

            Los campos recibidos por la clases DRPCSpout se refieren a ("Nombre de la funcion en String", en caso de
            ser un localDRPC colocar un new LocalDRPC() ).

            En caso de enviar a una cluster remote usar la siguiente declaracion por el Spout (DRPCSpout)
           y bolt (ReturnResults) son clases definidas por Storm core. */

        DRPCSpout spout = new DRPCSpout("FrankManualDRPC");

        /*  Configurando el numero de Excecutors (Threads) y task (bolts/spouts instances metodos execute()/nextTuple())
            ejemplo:  builder.setSpout("Spoutdrpc", spout,3);

            El numero 3 es la cantidad de excecuters para spoutdrpc. El numero de excecuters es igual al numero de task
             si lo configuras de esta manera.

             Otra manera de variar el numero de task mientras se tiene el mismo numero de excecuters es:
             builder.setBolt("FrankFilterUserDRPC", new FrankFilterUserDRPC(), 3).setNumTasks(4).shuffleGrouping("Spoutdrpc");

         */
        builder.setSpout("Spoutdrpc", spout,3);
        builder.setBolt("FrankSelectScenaryCollDRPC", new FrankSelectScenaryCollDRPC(), 6).shuffleGrouping("Spoutdrpc");
        builder.setBolt("FrankFilterUserDRPC", new FrankFilterUserDRPC(), 3).setNumTasks(6).shuffleGrouping("FrankSelectScenaryCollDRPC");
        builder.setBolt("FrankAlgoritmTimeGroupDRPC", new FrankAlgoritmTimeGroupDRPC(), 6).shuffleGrouping("FrankFilterUserDRPC","Ultrabolt");
        builder.setBolt("FrankFilterScenaryMandatoryDRPC", new FrankFilterScenaryMandatoryDRPC(), 6).shuffleGrouping("FrankAlgoritmTimeGroupDRPC","ScenaryGroup");
        builder.setBolt("FrankSaveGroupScenaryCollDRPC", new FrankSaveGroupScenaryDRPC(), 6).shuffleGrouping("FrankFilterScenaryMandatoryDRPC","SaveScenary");

        builder.setBolt("FrankSaveGroupTimeDRPC", new FrankSaveGroupTimeDRPC(), 3).shuffleGrouping("FrankFilterScenaryMandatoryDRPC", "SaveGroup");
        builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("FrankAlgoritmTimeGroupDRPC").shuffleGrouping("FrankFilterUserDRPC");

        //LocalCluster cluster = new LocalCluster();
        Config conf = new Config();


        // Configurando el numero de workers
        conf.setNumWorkers(3);// Cada worker representa una JVM, por default el numero de Worker es 1.
        //cluster.submitTopology("UserFrankManualDRPC", conf, builder.createTopology());
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());

/*        Document doc_user = new Document();
        LOG.info("Test topology Cliente Java");
        ObjectId id = new ObjectId();
        doc_user.append("id", id);
        doc_user.append("TOKEN" , "db8e1a907d861cc132b4fc26d8f7b207a43bc6b192761756d48ef00326b53ee2");
        doc_user.append("MSISDN", "34633998875" );
        doc_user.append("IMEI", "355828063300862");
        doc_user.append("IMSI", "214040101801031");
        doc_user.append("XDRS",59);
        doc_user.append("SCENARY", "MO");
        LOG.info("Test topology Cliente Java: "+doc_user.toJson());
*/
        //LOG.info("Results for DRPC debug:"+drpc.execute("UserFrankManualDRPC",  JSON.serialize(doc_user)));
        //Thread.sleep(10000);
        //cluster.shutdown();
        //drpc.shutdown();

    }


}
