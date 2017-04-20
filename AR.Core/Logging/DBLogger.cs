using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Npgsql;
using AR.Core.Types;
using UnityEngine.Networking;
using UnityEngine;

namespace AR.Core.Logging
{
#if EMBEDED
    public class DBLogger
    {
        private String ConnectionString
        {
            get
            {
                return String.Format("Host={0}; Database={1}; User ID={2}; Password={3}",
                    SystemSetup.DBLog_Server, SystemSetup.DBLog_DBName, SystemSetup.DBLog_Username, SystemSetup.DBLog_Password);
            }


        }
        private static DBLogger instance;
        private DBLogger() { }

        public static DBLogger getInstance()
        {
            if (instance == null)
                instance = new DBLogger();

            return instance;
        }

        public Boolean LogMessage(LoggingLevels loglevel, String Message, String Module = "", String Version = "")
        {
            NpgsqlConnection conn = new NpgsqlConnection(ConnectionString);
            try
            {

                conn.Open();

                NpgsqlCommand command = conn.CreateCommand();
                string sql = String.Format("INSERT INTO {0} (level, module, message, version) values('{1}', '{2}', '{3}', '{4}')", 
                    SystemSetup.DBLog_SchemaTableName, loglevel.ToString(), Module, Message, Version);
                command.CommandText = sql;

                var rows = command.ExecuteNonQuery();
                conn.Close();

                return rows == 1;
            }
            catch(Exception exp)
            {
                return false;
            }

        }

    }
#else
    public class DBLogger : MonoBehaviour
    {
        private String PostURL = "http://" + SystemSetup.DBLog_Server + ":3000/logs"; 

        private static DBLogger instance;
        private DBLogger() { }

        public static DBLogger getInstance()
        {
            if (instance == null)
                instance = new DBLogger();

            return instance;
        }

        public Boolean LogMessage(LoggingLevels loglevel, String Message, String Module = "", String Version = "")
        {

            if (SystemSetup.ENABLE_PRINT_LOGGING)
                print(Message);


            //don't post remote logs if running in prod only for debugging
            if (!SystemSetup.ENABLE_REMOTE_LOGGING)
                return true;

            try
            {

      
                Dictionary<String, String> myLoad = new Dictionary<string, string>();
                myLoad.Add("level", loglevel.ToString());
                myLoad.Add("module", Module);
                myLoad.Add("message", Message);
                myLoad.Add("version", Version);

                UnityWebRequest.Post(PostURL, myLoad).Send();  

                return true;
            }
            catch (Exception exp)
            {
                UnityEngine.Debug.LogError(exp.Message);
                return false;
            }

        }

    }
#endif

}
