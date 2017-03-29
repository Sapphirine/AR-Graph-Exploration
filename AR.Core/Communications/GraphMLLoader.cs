using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Xml;
using AR.Core.Logging;
using ARTypes = AR.Core.Types;
#if EMBEDED
using System.Net;
#else
using UnityEngine.Networking;
#endif


namespace AR.Core.Communications
{
    /// <summary>
    /// Graph Factory from GraphMLFile
    /// </summary>
    public static class GraphMLGraphFactory
    {

#if EMBEDED

        public static Graph.Graph GetGraphFromURL(String URL)
        {
            Graph.Graph retGraph = new Graph.Graph();

            try
            {

                string result = "";

                using (var client = new WebClient())
                {
                    result = client.DownloadString(URL);

                }


                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.LoadXml(result);

                // Get elements
                XmlNodeList Nodes = xmlDoc.GetElementsByTagName("node");
                XmlNodeList Edges = xmlDoc.GetElementsByTagName("edge");




                foreach (XmlNode Node in Nodes)
                {
                    var nextNode = retGraph.AddNodes(Node.Attributes["id"].Value, null);

                    foreach (System.Xml.XmlAttribute a in Node.Attributes)
                    {
                        switch (a.Name)
                        {
                            case "id":
                                continue;
                            case "Weight":
                                nextNode.Weight = Convert.ToInt32(a.Value);
                                continue;
                            default:
                                nextNode.Properties.Add(a.Name, a.Value);
                                continue;
                        }
                    }

                }


                foreach (XmlNode Edge in Edges)
                {
                    Graph.Node sNode = null, tNode = null;

                    //oops edge with no node created.
                    if (Edge.Attributes["source"] == null)
                        sNode = retGraph.AddNodes(Edge.Attributes["id"].Value, null);
                    else
                        sNode = retGraph.GetNode(Edge.Attributes["source"].Value);

                    //oops edge with no node created.
                    if (Edge.Attributes["target"] == null)
                        tNode = retGraph.AddNodes(Edge.Attributes["id"].Value, null);
                    else
                        tNode = retGraph.GetNode(Edge.Attributes["target"].Value);

                    var nextEdge = retGraph.AddEdges(sNode, tNode);

                    foreach (System.Xml.XmlAttribute a in Edge.Attributes)
                    {
                        switch (a.Name)
                        {
                            case "id":
                                nextEdge.Label = a.Value;
                                continue;
                            case "Weight":
                                nextEdge.Weight = Convert.ToInt32(a.Value);
                                continue;
                            default:
                                nextEdge.Properties.Add(a.Name, a.Value);
                                continue;
                        }
                    }

                }




            }
            catch (Exception exp)
            {
                return null;

            }
            return retGraph;
        }
        public static Graph.Graph GetGraphFromFile(String FileName)
        {
            Graph.Graph retGraph = new Graph.Graph();

            try
            {
            
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load(FileName);

                // Get elements
                XmlNodeList Nodes = xmlDoc.GetElementsByTagName("node");
                XmlNodeList Edges = xmlDoc.GetElementsByTagName("edge");


                

                foreach (XmlNode Node in Nodes)
                {
                    var nextNode = retGraph.AddNodes(Node.Attributes["id"].Value, null);
                    
                    foreach (System.Xml.XmlAttribute a in Node.Attributes)
                    {
                        switch (a.Name)
                        {
                            case "id":
                                continue;
                            case "Weight":
                                nextNode.Weight = Convert.ToInt32(a.Value);
                                continue;
                            default:
                                nextNode.Properties.Add(a.Name, a.Value);
                                continue;
                        }
                    }

                }


                foreach (XmlNode Edge in Edges)
                {
                    Graph.Node sNode = null, tNode = null;

                    //oops edge with no node created.
                    if (Edge.Attributes["source"] == null)
                        sNode = retGraph.AddNodes(Edge.Attributes["id"].Value, null);
                    else
                        sNode = retGraph.GetNode(Edge.Attributes["source"].Value);

                    //oops edge with no node created.
                    if (Edge.Attributes["target"] == null)
                        tNode = retGraph.AddNodes(Edge.Attributes["id"].Value, null);
                    else
                        tNode = retGraph.GetNode(Edge.Attributes["target"].Value);

                    var nextEdge = retGraph.AddEdges(sNode, tNode);
             
                    foreach (System.Xml.XmlAttribute a in Edge.Attributes)
                    {
                        switch (a.Name)
                        {
                            case "id":
                                nextEdge.Label = a.Value;
                                continue;
                            case "Weight":
                                nextEdge.Weight = Convert.ToInt32(a.Value);
                                continue;
                            default:
                                nextEdge.Properties.Add(a.Name, a.Value);
                                continue; 
                        }
                    }

                }




            }
            catch (Exception exp)
            {
                return null;

            }
            return retGraph;
        }
  
#else

        public static void GetGraphFromURL(Graph.Graph retGraph, String URL)
        {
            var myLogs = AR.Core.Logging.DBLogger.getInstance();
            myLogs.LogMessage(ARTypes.LoggingLevels.Verbose, "Init GetGraphFromURL", 
                Module: "GraphMLGraphFactory.GetGraphFromURL", Version: "ALPHA");

            try
            {
                UnityWebRequest www = UnityWebRequest.Get(URL);
                www.Send();
                while (!www.isDone)
                {
                    //intentionally blank
                }
                
                string result = www.downloadHandler.text;


                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.LoadXml(result);

                // Get elements
                XmlNodeList Nodes = xmlDoc.GetElementsByTagName("node");
                XmlNodeList Edges = xmlDoc.GetElementsByTagName("edge");

                foreach (XmlNode Node in Nodes)
                {
                    var nextNode = retGraph.AddNodes(Node.Attributes["id"].Value, null);

                    foreach (System.Xml.XmlAttribute a in Node.Attributes)
                    {
                        switch (a.Name)
                        {
                            case "id":
                                continue;
                            case "Weight":
                                nextNode.Weight = Convert.ToInt32(a.Value);
                                continue;
                            default:
                                nextNode.Properties.Add(a.Name, a.Value);
                                continue;
                        }
                    }

                }


                foreach (XmlNode Edge in Edges)
                {
                    Graph.Node sNode = null, tNode = null;

                    //oops edge with no node created.
                    if (Edge.Attributes["source"] == null)
                        sNode = retGraph.AddNodes(Edge.Attributes["id"].Value, null);
                    else
                        sNode = retGraph.GetNode(Edge.Attributes["source"].Value);

                    //oops edge with no node created.
                    if (Edge.Attributes["target"] == null)
                        tNode = retGraph.AddNodes(Edge.Attributes["id"].Value, null);
                    else
                        tNode = retGraph.GetNode(Edge.Attributes["target"].Value);

                    var nextEdge = retGraph.AddEdges(sNode, tNode);

                    foreach (System.Xml.XmlAttribute a in Edge.Attributes)
                    {
                        switch (a.Name)
                        {
                            case "id":
                                nextEdge.Label = a.Value;
                                continue;
                            case "Weight":
                                nextEdge.Weight = Convert.ToInt32(a.Value);
                                continue;
                            default:
                                nextEdge.Properties.Add(a.Name, a.Value);
                                continue;
                        }
                    }

                }




            }
            catch (Exception exp)
            {
                myLogs.LogMessage(ARTypes.LoggingLevels.Error, "Init GetGraphFromURL Exception: " + exp.Message,
    Module: "GraphMLGraphFactory.GetGraphFromURL", Version: "ALPHA");
                return;

            }
            return;
        }

        public static Graph.Graph GetGraphFromFile(String FileName)
        {
            Graph.Graph retGraph = new Graph.Graph();

            try
            {

                String Results = "";

                /*using (StreamReader sr = new StreamReader(new FileStream(FileName, FileMode.Open)))
                {
                    Results = sr.ReadToEnd();
                    sr.Close();
                }*/

                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.LoadXml(Results);

                // Get elements
                XmlNodeList Nodes = xmlDoc.GetElementsByTagName("node");
                XmlNodeList Edges = xmlDoc.GetElementsByTagName("edge");




                foreach (XmlNode Node in Nodes)
                {
                    var nextNode = retGraph.AddNodes(Node.Attributes["id"].Value, null);

                    foreach (System.Xml.XmlAttribute a in Node.Attributes)
                    {
                        switch (a.Name)
                        {
                            case "id":
                                continue;
                            case "Weight":
                                nextNode.Weight = Convert.ToInt32(a.Value);
                                continue;
                            default:
                                nextNode.Properties.Add(a.Name, a.Value);
                                continue;
                        }
                    }

                }


                foreach (XmlNode Edge in Edges)
                {
                    Graph.Node sNode = null, tNode = null;

                    //oops edge with no node created.
                    if (Edge.Attributes["source"] == null)
                        sNode = retGraph.AddNodes(Edge.Attributes["id"].Value, null);
                    else
                        sNode = retGraph.GetNode(Edge.Attributes["source"].Value);

                    //oops edge with no node created.
                    if (Edge.Attributes["target"] == null)
                        tNode = retGraph.AddNodes(Edge.Attributes["id"].Value, null);
                    else
                        tNode = retGraph.GetNode(Edge.Attributes["target"].Value);

                    var nextEdge = retGraph.AddEdges(sNode, tNode);

                    foreach (System.Xml.XmlAttribute a in Edge.Attributes)
                    {
                        switch (a.Name)
                        {
                            case "id":
                                nextEdge.Label = a.Value;
                                continue;
                            case "Weight":
                                nextEdge.Weight = Convert.ToInt32(a.Value);
                                continue;
                            default:
                                nextEdge.Properties.Add(a.Name, a.Value);
                                continue;
                        }
                    }

                }




            }
            catch (Exception exp)
            {
                return null;

            }
            return retGraph;
        }

#endif



    }
}
