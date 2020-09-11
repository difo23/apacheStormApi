package com.ayscom.minetur.config;

/**
 * Created by Christian on 30/6/15.
 */
public class Config {
    public static class Database {
        public static String CRUDE_XDR_SERVER_IP = "localhost";
        public static int CRUDE_XDR_SERVER_PORT = 27017;
        public static String CRUDE_XDR_DATABASE = "ministerio";
        public static String CRUDE_XDR_COLLECTION = "xdrs_crudos_v2";

        public static String RICH_XDR_SERVER_IP = "localhost";
        public static int RICH_XDR_SERVER_PORT = 27017;

        public static String MO_CALL_SERVER_IP = "localhost";
        public static int MO_CALL_SERVER_PORT = 27017;
        public static String MO_CALL_XDR_DATABASE = "ministerio";
        public static String MO_CALL_XDR_COLLECTION = "mo_call_xdrs_v2";

        public static String MT_CALL_SERVER_IP = "localhost";
        public static int MT_CALL_SERVER_PORT = 27017;
        public static String MT_CALL_XDR_DATABASE = "ministerio";
        public static String MT_CALL_XDR_COLLECTION = "mt_call_xdrs";

        public static String MO_SMS_SERVER_IP = "localhost";
        public static int MO_SMS_SERVER_PORT = 27017;
        public static String MO_SMS_XDR_DATABASE = "ministerio";
        public static String MO_SMS_XDR_COLLECTION = "mo_sms_xdrs";

        public static String MT_SMS_SERVER_IP = "localhost";
        public static int MT_SMS_SERVER_PORT = 27017;
        public static String MT_SMS_XDR_DATABASE = "ministerio";
        public static String MT_SMS_XDR_COLLECTION = "mo_sms_xdrs";


        public static String MOBILITY_SERVER_IP = "localhost";
        public static int MOBILITY_SERVER_PORT = 27017;
        public static String MOBILITY_XDR_DATABASE = "ministerio";
        public static String MOBILITY_XDR_COLLECTION = "mobility_xdrs";


        public static String GROUPS_FINAL_SERVER_IP = "localhost";
        public static int GROUPS_FINAL_SERVER_PORT = 27017;
        public static String GROUPS_FINAL_DATABASE = "ministerio";
        public static String GROUPS_FINAL_COLLECTION = "group_final";

        public static String GROUPS_REFUSE_SERVER_IP = "localhost";
        public static int GROUPS_REFUSE_SERVER_PORT = 27017;
        public static String GROUPS_REFUSE_DATABASE = "ministerio";
        public static String GROUPS_REFUSE_COLLECTION = "group_refuse";

        public static String GROUPS_TEMP_SERVER_IP = "localhost";
        public static int GROUPS_TEMP_SERVER_PORT = 27017;
        public static String GROUPS_TEMP_DATABASE = "ministerio";
        public static String GROUPS_TEMP_COLLECTION = "group_temp";

        public static String FRANK_SERVER_IP = "localhost";
        public static int FRANK_SERVER_PORT = 27017;
        public static String FRANK_DATABASE = "ministerio";

        public static String USERS_SERVER_IP = "localhost";
        public static int USERS_SERVER_PORT = 27017;
        public static String USERS_DATABASE = "ministerio";
        public static String USERS_COLLECTION = "users";

        public static String TOKEN_SERVER_IP = "localhost";
        public static int TOKEN_SERVER_PORT = 27017;
        public static String TOKEN_DATABASE = "ministerio";
        public static String TOKEN_COLLECTION = "api_jupiter_token";

        public static String MEASUREMENTS_SERVER_IP = "localhost";
        public static int MEASUREMENTS_SERVER_PORT = 27017;
        public static String MEASUREMENTS_DATABASE = "ministerio";
        public static String MEASUREMENTS_COLLECTION = "measurements";
    }

    public static class JupiterApi {
        public static String QUERY_URL = "https://10.20.7.100/jupiter/query";
    }




}
