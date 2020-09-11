package com.ayscom.minetur.config;

/**
 * Created by Christian on 30/6/15.
 */
public class Protocols {
    public static class IuCs {
        public static class AInterface{
            public static String Call = "Iu-CS/A-Interface.Call";
            public static String CallSetup = "Iu-CS/A-Interface.CallSetup";
        }
        public static String Message = "Iu-CS.IuMessage";
        public static String Other = "Iu-CS.Other";
        public static String RAN = "Iu-CS.RAN";
    }

    public static class Map {
        public static String Transaction = "MAP.Transaction";
    }

    public static class S1AP {
        public static String Other = "S1AP.Other";
        public static String Paging = "S1AP Paging.Paging";
    }

    public static class ISUP {
        public static String Call = "ISUP.Call";
    }
}
