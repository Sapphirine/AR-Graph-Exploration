#if NOT_USED

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Data.SQLite;
using System.Data;

namespace AR.Core.Graph
{



    public class GraphSqlite
    {
        public SQLiteConnection m_dbConnection;


        public DataTable GetDataTable(string sql)
        {
            DataTable dt = new DataTable();
            try
            {

                SQLiteCommand cmd = new SQLiteCommand(sql, m_dbConnection);
                cmd.CommandText = sql;
                SQLiteDataReader reader = cmd.ExecuteReader();
                dt.Load(reader);
                reader.Close();
            }

            catch (Exception e)
            {
                throw new Exception(e.Message);
            }

            return dt;

        }

        public GraphSqlite(FileInfo LocalPath = null)
        {
            if (LocalPath == null)
                m_dbConnection = new SQLiteConnection("Data Source=:memory:");
            else
            {
                SQLiteConnection.CreateFile(LocalPath.FullName);
                m_dbConnection = new SQLiteConnection("Data Source=MyDatabase.sqlite;Version=3;");
            }

            m_dbConnection.Open();

            CreateTables();

        }
        public void CreateTables()
        {
            using (SQLiteTransaction tr = m_dbConnection.BeginTransaction())
            {
                using (SQLiteCommand cmd = m_dbConnection.CreateCommand())
                {

                    cmd.Transaction = tr;
                    //Nodes table
                    cmd.CommandText = "DROP TABLE IF EXISTS Nodes";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"CREATE TABLE Nodes(ID INTEGER PRIMARY KEY, 
                                        Label TEXT, Weight REAL)";
                    cmd.ExecuteNonQuery();

                    //Edges table
                    cmd.CommandText = "DROP TABLE IF EXISTS Edges";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"CREATE TABLE Edges(ID INTEGER PRIMARY KEY, 
                                        Label TEXT, Weight REAL)";
                    cmd.ExecuteNonQuery();

                    //Nodes-> EdgesOut  table
                    cmd.CommandText = "DROP TABLE IF EXISTS NodesEdgesOut";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"CREATE TABLE NodesEdgesOut(ID INTEGER PRIMARY KEY, NodesID INTEGER, EdgesID INTEGER,
                                        Label TEXT, Weight REAL)";
                    cmd.ExecuteNonQuery();

                    //Nodes-> EdgesIn table
                    cmd.CommandText = "DROP TABLE IF EXISTS NodesEdgesIn";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"CREATE TABLE NodesEdgesIn(ID INTEGER PRIMARY KEY, NodesID INTEGER, EdgesID INTEGER,
                                        Label TEXT, Weight REAL)";
                    cmd.ExecuteNonQuery();

                    //Nodes-> Properties table
                    cmd.CommandText = "DROP TABLE IF EXISTS NodesProperties";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"CREATE TABLE NodesProperties(ID INTEGER PRIMARY KEY, NodesID INTEGER, PropertyName TEXT,
                                        PropertyValue TEXT, PropertyType TEXT)";
                    cmd.ExecuteNonQuery();

                    //Nodes-> Properties table
                    cmd.CommandText = "DROP TABLE IF EXISTS EdgesProperties";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = @"CREATE TABLE EdgesProperties(ID INTEGER PRIMARY KEY, EdgesID INTEGER, PropertyName TEXT,
                                        PropertyValue TEXT, PropertyType TEXT)";
                    cmd.ExecuteNonQuery();

                }

                tr.Commit();
            }



        }

        public void addEdges(Edge edge)
        {
            using (SQLiteTransaction tr = m_dbConnection.BeginTransaction())
            {
                using (SQLiteCommand cmd = m_dbConnection.CreateCommand())
                {

                    cmd.Transaction = tr;
                    //Nodes table
                    cmd.CommandText = "INSERT INTO Edges (ID, Label , Weight) VALUES ($ID, $Label , $Weight) ";

                    cmd.Parameters.AddWithValue("$ID", edge.ID);
                    cmd.Parameters.AddWithValue("$Label", edge.Label);
                    cmd.Parameters.AddWithValue("$Weight", edge.Weight);
                    cmd.ExecuteNonQuery();


                    cmd.CommandText = "INSERT INTO NodesEdgesIn (NodesID , EdgesID , Label , Weight ) VALUES ($NodesID , $EdgesID , $Label , $Weight ) ";

                    cmd.Parameters.AddWithValue("$NodesID", edge.EndNode.ID);
                    cmd.Parameters.AddWithValue("$EdgesID", edge.ID);
                    cmd.Parameters.AddWithValue("$Label", edge.Label);
                    cmd.Parameters.AddWithValue("$Label", edge.Weight);
                    cmd.ExecuteNonQuery();


                    cmd.CommandText = "INSERT INTO NodesEdgesout (NodesID , EdgesID , Label , Weight ) VALUES ($NodesID , $EdgesID , $Label , $Weight ) ";

                    cmd.Parameters.AddWithValue("$NodesID", edge.StartNode.ID);
                    cmd.Parameters.AddWithValue("$EdgesID", edge.ID);
                    cmd.Parameters.AddWithValue("$Label", edge.Label);
                    cmd.Parameters.AddWithValue("$Label", edge.Weight);
                    cmd.ExecuteNonQuery();

                    foreach (var prop in edge.Properties)
                    {
                        cmd.CommandText = "INSERT INTO EdgesProperties (EdgesID , PropertyName ,PropertyValue , PropertyType) VALUES ($EdgesID , $PropertyName , $PropertyValue , $PropertyType) ";
                        cmd.Parameters.AddWithValue("$EdgesID", edge.ID);
                        cmd.Parameters.AddWithValue("$PropertyName", prop.Key);
                        cmd.Parameters.AddWithValue("$PropertyValue", prop.Value.ToString());
                        cmd.Parameters.AddWithValue("$PropertyValue", prop.Value.GetType().ToString());
                        cmd.ExecuteNonQuery();
                    }

                }

                tr.Commit();
            }

        }
        public void deleteEdges(Edge edge)
        {
            using (SQLiteTransaction tr = m_dbConnection.BeginTransaction())
            {
                using (SQLiteCommand cmd = m_dbConnection.CreateCommand())
                {

                    cmd.Transaction = tr;
                    //Nodes table
                    cmd.CommandText = "DELETE FROM Edges WHERE ID = $ID";
                    cmd.Parameters.AddWithValue("$ID", edge.ID);
                    cmd.ExecuteNonQuery();


                    cmd.CommandText = "DELETE FROM NodesEdgesIn EdgesID = $EdgesID ";
                    cmd.Parameters.AddWithValue("$EdgesID", edge.ID);
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "DELETE FROM NodesEdgesout EdgesID = $EdgesID ";
                    cmd.Parameters.AddWithValue("$EdgesID", edge.ID);
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "DELETE FROM EdgesProperties EdgesID = $EdgesID ";
                    cmd.Parameters.AddWithValue("$EdgesID", edge.ID);
                    cmd.ExecuteNonQuery();

                }

                tr.Commit();
            }



        }
        public void addNode(Node node)
        {
            using (SQLiteTransaction tr = m_dbConnection.BeginTransaction())
            {
                using (SQLiteCommand cmd = m_dbConnection.CreateCommand())
                {

                    cmd.Transaction = tr;

                    cmd.Transaction = tr;
                    //Nodes table
                    cmd.CommandText = "INSERT INTO Nodes (ID, Label , Weight) VALUES ($ID, $Label , $Weight) ";

                    cmd.Parameters.AddWithValue("$ID", node.ID);
                    cmd.Parameters.AddWithValue("$Label", node.Label);
                    cmd.Parameters.AddWithValue("$Weight", node.Weight);
                    cmd.ExecuteNonQuery();

                    foreach (var x in node.Properties)
                    {
                        cmd.CommandText = "INSERT INTO NodesProperties (NodesID, PropertyName, PropertyValue , PropertyType) VALUES ($NodesID, $PropertyName, $PropertyValue, $PropertyType)";
                        cmd.Parameters.AddWithValue("$NodesID", node.ID);
                        cmd.Parameters.AddWithValue("$PropertyName", x.Key);
                        cmd.Parameters.AddWithValue("$PropertyValue", x.Value);
                        cmd.Parameters.AddWithValue("$PropertyValue", x.GetType().ToString());
                        cmd.ExecuteNonQuery();
                    }


                }

                tr.Commit();
            }



        }
        public void deleteNodes(Node node)
        {
            using (SQLiteTransaction tr = m_dbConnection.BeginTransaction())
            {
                using (SQLiteCommand cmd = m_dbConnection.CreateCommand())
                {

                    cmd.Transaction = tr;
                    //Nodes table
                    cmd.CommandText = "DELETE FROM Nodes WHERE ID = $ID";
                    cmd.Parameters.AddWithValue("$ID", node.ID);
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "DELETE FROM NodesEdgesIn NodeID = $NodeID ";
                    cmd.Parameters.AddWithValue("$NodeID", node.ID);
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "DELETE FROM NodesEdgesout NodeID = $NodeID ";
                    cmd.Parameters.AddWithValue("$EdgesID", node.ID);
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = "DELETE FROM NodesProperties NodeID = $NodeID ";
                    cmd.Parameters.AddWithValue("$NodeID", node.ID);
                    cmd.ExecuteNonQuery();

                }

                tr.Commit();
            }



        }
        public List<Int64> getEdgeList(String selectStatement)
        {

            var myData = GetDataTable(selectStatement);

             /*List<Int64> list = (from row in myData.AsEnumerable()
                                 select row.Field<Int64>("ID")).ToList<Int64>();*/
             return null; 

        }
        public List<Int64> getNodeList(String selectStatement)
        {

            var myData = GetDataTable(selectStatement);

            /*List<Int64> list = (from row in myData.AsEnumerable()
                                select row.Field<Int64>("ID")).ToList<Int64>();*/
            return null; 
  
        }


    }
}

#endif