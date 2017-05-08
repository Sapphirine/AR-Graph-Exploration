
#define NEWTONSOFT


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AR.Core.Types
{
    public static class SystemSetup
    {


        public static Boolean ENABLE_REMOTE_LOGGING = false;
        public static Boolean ENABLE_PRINT_LOGGING = false;


        public static String DBLog_Server = "bphphome.dyndns.org";
        public static String DBLog_Username = "ARCoreLogger";
        public static String DBLog_Password = "Logger1234!";
        public static String DBLog_DBName = "ARCore";
        public static String DBLog_SchemaTableName = "public.logs";

        public static Boolean ConnectFinalProject = false; //flip to go from local to prod

        public static Boolean Neo4j_Authless = ConnectFinalProject ? false: true;
        public static String Neo4j_Server = ConnectFinalProject ? "ec2-52-23-203-124.compute-1.amazonaws.com": "bphphome.dyndns.org";
        public static String Neo4j_Username = "neo4j";
        public static String Neo4j_Password = ConnectFinalProject ? "neo4j2" : "Logger1234!";


        public static List<String> RootSpeechKeywords = new List<string>() { "Graph Properties", "Dense", "Reset", "Stop", "Load Simple Graph", "Load Sample Graph", "Load Database Graph",
            "Depth First Search", "Breadth First Search", "Move Nodes", "Disable Speach", "Enable Speach", "List Commands", "Hide Edges", "Show Edges", "Hide Nodes", "Show Nodes" };

    }
}
