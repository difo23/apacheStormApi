package com.ayscom.minetur.config;

/**
 * Created by lramirez on 6/08/15.
 */
public class TopologiesIDs {

    public static class TopologiesNames {
        public static String FRANK_DRPC_TOPOLOGY = "FrankManualDRPC";
        public static String SCENARY_TOPOLOGY = "ScenaryTopology";
        public static String MACHINE_TOPOLOGY= "MachineStorm";
        public static String FRANK_TOPOLOGY = "FrankReloadTopology";
    }

    public static class FrankStreamNames {
        public static String FRANK_FILTER_USER_ALGORITM_STREAM = "Ultrabolt";
        public static String FRANK_ALGORITM_SCENARY_GROUP_STREAM = "ScenaryGroup";
        public static String SCENARY_GROUP_SAVE_GROUP_STREAM = "SaveGroup";
        public static String SCENARY_GROUP_SAVE_SCENARY_STREAM = "SaveScenary";

        public static String FRANK_USER_SPOUT_TO_SCENARY_SELECT = "0012";


        public static String FRANK_SCENARY_SELECT_MO_TO_RECURSIVE_ALGORITM = "1001";
        public static String FRANK_SCENARY_SELECT_MT_TO_RECURSIVE_ALGORITM = "1031";
        public static String FRANK_SCENARY_SELECT_MO_SMS_TO_RECURSIVE_ALGORITM = "1041";
        public static String FRANK_SCENARY_SELECT_MT_SMS_TO_RECURSIVE_ALGORITM = "1051";
        public static String FRANK_SCENARY_SELECT_MOBILITY_TO_RECURSIVE_ALGORITM = "1061";

        public static String FRANK_RECURSIVE_ALGORTIM_TO_SCENARY_MADATORY= "1002";
        public static String FRANK_LOGS_USER_TO_SCENARY_TEMP = "1003";
        public static String FRANK_SCENARY_MANDATORY_TO_LOGS_USER = "1008";
        public static String FRANK_SCENARY_MANDATORY_TO_REFUSE_GROUPS = "1009";
        public static String FRANK_LOGS_USER_TO_SCENARY_FINAL = "1004";
        public static String FRANK_SCENARY_FINAL_TO_SAVE_GROUP = "1005";
        public static String FRANK_SCENARY_FINAL_TO_DELETE_XDRS = "1006";
        public static String FRANK_SCENARY_TEMP_TO_SAVE_GROUP = "1007";

    }

    public static class FrankSpoutsIDs {
        public static String SPOUT_DRPC = "Spoutdrpc";
        public static String FRANK_SPOUT_USERS = "FrankUserSpout";

    }

    public static class FrankBoltsIDs {

        //Topology FrankReloadTopology
        public static String FRANK_RECURSIVE_BOLT = "FrankRecusiveBolt";
        public static String FRANK_SAVE_GROUPS_TEMP_BOLT = "FrankSaveTempGroupsBolt";
        public static String FRANK_SAVE_GROUPS_FINAL_BOLT = "FrankSaveFinalGroupsBolt";
        public static String FRANK_SCENARY_MANDATORY_BOLT = "FrankScenaryMandatoryBolt";
        public static String FRANK_SCENARY_SELECT_MO_BOLT = "FrankScenarySelectMOBolt";
        public static String FRANK_SCENARY_SELECT_MT_BOLT = "FrankScenarySelectMTBolt";
        public static String FRANK_SCENARY_SELECT_MO_SMS_BOLT = "FrankScenarySelectMOSMSBolt";
        public static String FRANK_SCENARY_SELECT_MT_SMS_BOLT = "FrankScenarySelectMTSMSBolt";
        public static String FRANK_SCENARY_SELECT_MOBILITY_BOLT = "FrankScenarySelectMobilityBolt";

        public static String FRANK_SCENARY_FINAL_BOLT= "FrankScenaryFinalBolt";
        public static String FRANK_SCENARY_TEMP_BOLT= "FrankScenaryTempBolt";
        public static String FRANK_DELETE_XDRS_BOLT= "FrankDeleteXDRSBolt";
        public static String FRANK_REFUSE_GROUPS_BOLT= "FrankSaveRefuseGroupsBolt";
        public static String FRANK_LOGS_USER_BOLT= "FrankLogsUserBolt";
        //End FranReloadTopology

        public static String FRANK_FILTER_USER_DRPC = "FrankFilterUserDRPC";
        public static String FRANK_ALGORITM_TIME_GROUP_DRPC = "FrankAlgoritmTimeGroupDRPC";
        public static String FRANK_SAVE_GROUP_TIME_DRPC = "FrankSaveGroupTimeDRPC";
        public static String RETURN = "return";
        public static String FRANK_FILTER_SCENARY_MANDATORY_DRPC = "FrankFilterScenaryMandatoryDRPC";
        public static String FRANK_SELECT_SCENARY_COLL_DRPC = "FrankSelectScenaryCollDRPC";
        public static String FRANK_SAVE_GROUP_SCENARY_DRPC = "FrankSaveGroupScenaryDRPC";
    }


    public static class ScenSpoutsIDS {
        public static String CRUDE_XDR_SPOUT = "crude_xdr_spout";

    }

    public static class ScenBoltsIDS {
        public static String IU_CS_AINTERFACE_CALL_BOLT = "iucsainterfacecallbolt";
        public static String IU_CS_AINTERFACE_CALL_SETUP_BOLT = "iucsainterfacecallsetupbolt";
        public static String IU_CS_MESSAGE_BOLT = "iu_cs_message_bolt";

        public static String IU_CS_RAN_BOLT = "iu_cs_ran_bolt";
        public static String IU_CS_OTHER_BOLT = "iu_cs_other_bolt";
        public static String SIAP_OTHER_BOLT ="s1ap_other_bolt";
        public static String S1AP_PAGING_BOLT ="s1ap_paging_bolt";
        public static String ISUP_CALL_BOLT ="isup_call_bolt";

    }
}
