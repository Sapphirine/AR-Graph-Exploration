
#define NEWTONSOFT


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AR.Core.Types
{
    public static class SystemSetup
    {


        public static Boolean ENABLE_REMOTE_LOGGING = true;
        public static Boolean ENABLE_PRINT_LOGGING = true;


        public static String DBLog_Server = "192.168.1.8";
        public static String DBLog_Username = "ARCoreLogger";
        public static String DBLog_Password = "Logger1234!";
        public static String DBLog_DBName = "ARCore";
        public static String DBLog_SchemaTableName = "public.logs";

        public static Boolean Neo4j_Authless = true;
        public static String Neo4j_Server = "192.168.1.8";
        public static String Neo4j_Username = "neo4j";
        public static String Neo4j_Password = "Logger1234!";

    }
}
