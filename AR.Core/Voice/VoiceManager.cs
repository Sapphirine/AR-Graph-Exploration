using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using AR.Core.Graph;
using UnityEngine;

namespace AR.Core.Voice
{
    public static class VoiceManager 
    {

        public static String RelayEdgeProperties(Edge e)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("Viewing Edge " + e.ID.ToString());
            sb.Append(" Which connects nodes " + e.StartNode.UserID.ToString() + " and " + e.EndNode.UserID.ToString() + ".");
            sb.Append("  The edge has the following properties ");


            //if Neo4j rip properties from 
            if (e.Neo4jPath != null)
            {
                if (e.Neo4jPath.Length > 0)
                {
                    String JsonOfProps = AR.Core.Communications.Neo4jConnector.getInstance().GetNodeProperties(e.ID.ToString());
                    var x = AR.Core.IO.SimpleJson.DeserializeObject<Dictionary<String, System.Object>>(JsonOfProps);
                    foreach (var kv in x)
                    {
                        sb.Append(kv.Key.ToString() + " " + kv.Value.ToString() + ".");
                    }
                }
            }
            else
            {
                foreach (var kv in e.Properties)
                {
                    sb.Append(kv.Key.ToString() + " " + kv.Value.ToString() + ".");
                }
            }

            return sb.ToString();

        }

        public static String RelayNodeProperties(Node n)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("Viewing Node " + n.ID.ToString());
            sb.Append(" Which has " + n.EdgesOut.Count.ToString() + " Outgoing edges and " + n.EdgesIn.Count.ToString() + " incomming edges.");
            sb.Append("  The node has the following properties ");


            //if Neo4j rip properties from 
            if (n.Neo4jPath != null)
                if(n.Neo4jPath.Length > 0)
                {
                    String JsonOfProps = AR.Core.Communications.Neo4jConnector.getInstance().GetNodeProperties(n.ID.ToString());
                    var x = AR.Core.IO.SimpleJson.DeserializeObject<Dictionary<String, System.Object>>(JsonOfProps);
                    foreach (var kv in x)
                    {
                        sb.Append(kv.Key.ToString() + " " + kv.Value.ToString() + ".");
                    }
                }

            foreach (var kv in n.Properties)
            {
                sb.Append(kv.Key.ToString() + " " + kv.Value.ToString() + ".");
            }
            return sb.ToString();
        }

        public static String RelayGraphProperties(Graph.Graph g)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("The Graph shown " + g.LABEL);
            sb.Append(" has " + g.AllEdges.Count.ToString() + " total edges and " + g.AllNodes.Count.ToString() + " total nodes.");
            return sb.ToString();
        }


    }
}
