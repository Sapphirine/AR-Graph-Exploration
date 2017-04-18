using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using UnityEngine;
using AR.Core.Types;


namespace AR.Core.Graph
{

    public class Graph : MonoBehaviour, IUnityVisualProperties
    {
        private Logging.DBLogger myLogs;
        private Speech.SpeechProcessing mySpeechEngine;
        private ARTouch.InteractibleManager myInteractibleManager;
        private ARTouch.GestureManager myGestureManager;
        public Boolean removeSpeach;



        public event EventHandler<TextArgs> Feedback;
        public void RaiseFeedback(string p)
        {
            EventHandler<TextArgs> handler = Feedback;
            if (handler != null && !removeSpeach)
            {
                handler(null, new TextArgs(p));

            }
        }

        public String LABEL = "Sample Graph";

        //Graph Properties

        public Dictionary<String, Node> AllNodes { get; set; }
        public Dictionary<UInt32, Edge> AllEdges { get; set; }
        //public GraphSqlite FlatGraph { get; set; }

        //From IUnityVisualProps

        public Color myColor { get; set; }
        public Mesh myMesh { get; set; }
        public Material myMaterial { get; set; }
        public Vector3 myLocation { get; set; }
        public Vector3 myRotation { get; set; }
        public Vector3 myScale { get; set; }
        public float mySize { get; set; }

        public Graph()
        {
            removeSpeach = false;
            AllNodes = new Dictionary<String, Node>();
            AllEdges = new Dictionary<UInt32, Edge>();
            myLogs = Logging.DBLogger.getInstance();
            myLogs.LogMessage(LoggingLevels.Verbose, "Graph Constructor Called", Module: "Graph.Start", Version: "ALPHA");

        }

        //this handles all the node select, add, remove commands a user would need

        public Node AddNodes(String Label = "", UnityVisualProperties opVisProp = null)
        {

            //Overide the default UserID if Label has been provided 
            //This ensures that the lookup from edge to Node matches!!!
            Node node = this.gameObject.AddComponent<Node>();

            if (Label != "") //added to match node labels to Unity Names
            {
                node.Label = Label;
            }
            //if (Label != "")
            //    node.UserID = Label;

            AllNodes.Add(node.UserID, node);
            //FlatGraph.addNode(node);
            return node;
        }
        public void DeleteNode(String UserID)
        {
             DeleteNode( GetNode(UserID));
        }
        public void DeleteNode(Node node)
        {
            DestroyImmediate(node.gameObject);

            foreach (var ed in node.EdgesIn)
            {
                DeleteEdges(ed.Key);
            }
            foreach (var ed in node.EdgesOut)
            {
                DeleteEdges(ed.Key);
            }

            //FlatGraph.deleteNodes(node);
            AllNodes.Remove(node.UserID);
        }
        public Node GetNode(String UserID)
        {

            foreach (Node n in AllNodes.Values)
            {
                if (n.Label == UserID)
                {
                    return n;
                }
            }

            return null;


            /*if (AllNodes.ContainsKey(UserID))
                return AllNodes[UserID];
            else
                return null;*/
        }

        public List<Node> GetNodes(List<String> UserIDS)
        {
            var retList = new List<Node>();
            foreach (var ID in UserIDS)
            {
                retList.Add(GetNode(ID));

            }
            return retList;
        }
        public List<Node> GetNodes(String SelectStatement)
        {

            var retList = new List<Node>();
            /*foreach (var ID in FlatGraph.getNodeList(SelectStatement))
            {
                retList.Add(GetNode(ID.ToString()));

            }*/
            return retList;
        }


        //this handles all the edge select, add, remove commands a user would need

        public Edge AddEdges(Node StartNode, Node EndNode, String Label = "")
        {
            
            //construct edge object
            Edge edge = this.gameObject.AddComponent<Edge>();
            edge.StartNode = StartNode;
            edge.EndNode = EndNode;


            StartNode.EdgesOut.Add(edge.ID, edge);
            EndNode.EdgesIn.Add(edge.ID, edge);
            AllEdges.Add(edge.ID, edge);
            //FlatGraph.addEdges(edge);

            return edge;
        }
        public void DeleteEdges(UInt32 ID)
        {
            DeleteEdges(getEdge(ID));
        }
        public void DeleteEdges(Edge edge)
        {
            edge.EndNode.EdgesIn.Remove(edge.ID);
            edge.StartNode.EdgesOut.Remove(edge.ID);
            //FlatGraph.deleteEdges(edge);
            AllEdges.Remove(edge.ID);
            DestroyImmediate(edge.gameObject);

        }

        public Edge getEdge(UInt32 ID)
        {
            return AllEdges[ID];
        }
        public List<Edge> getEdges(List<UInt32> IDS)
        {
            var retList = new List<Edge>();
            foreach (var ID in IDS)
            {
                retList.Add(getEdge(ID));

            }
            return retList;
        }
        public List<Edge> getEdges(String SelectStatement)
        {

            var retList = new List<Edge>();
            /*foreach (var ID in FlatGraph.getEdgeList(SelectStatement))
            {
                retList.Add(getEdge(Convert.ToUInt32(ID)));

            }*/
            return retList;
        }

        //Move Nodes Around
        public void RandomMoveAllNodes(Boolean in2d)
        {
            myLogs.LogMessage(LoggingLevels.Verbose, "Starting NodeMovements", Module: "Graph.RandomMoveAllNodes", Version: "ALPHA");
            System.Random r = new System.Random((int)DateTime.Now.Ticks);

            foreach (Node Nodes in AllNodes.Values)
            {
                var yShift = in2d ? 0 : r.Next(GraphConfiguration.GRAPHBOUNDINGBOX_ZMIN*100, GraphConfiguration.GRAPHBOUNDINGBOX_ZMAX*100)/100f;

                Nodes.MoveTo(r.Next(GraphConfiguration.GRAPHBOUNDINGBOX_XMIN*100, GraphConfiguration.GRAPHBOUNDINGBOX_XMAX*100)/100f,
                   yShift, r.Next(GraphConfiguration.GRAPHBOUNDINGBOX_ZMIN*100, GraphConfiguration.GRAPHBOUNDINGBOX_ZMAX*100)/100f
                   );
            }

        }
        public void ForceDirectedGraph(Boolean proj3d)
        {
            //Move all nodes around in 2D space x,y -> after force direct project into 3D
            RandomMoveAllNodes(true);
            
            for (int i = GraphConfiguration.FORCE_DIRECTED_ITERATIONS; i > 0; --i)
            {
                foreach (Node Nodes in AllNodes.Values)
                {
                    foreach (Node Nodes2 in AllNodes.Values)
                    {
                        if (Nodes.ID != Nodes2.ID)
                        {
                            if (Nodes.isDirectConnected(Nodes2))
                                Nodes.cuyrrentForceVector += GraphHelperFunctions.CalcAttractionForce(Nodes, Nodes2, GraphConfiguration.FORCE_DIRECTED_SPRING_LEN);
                            else
                                if (Nodes.isDirectConnected(Nodes2))
                                Nodes.cuyrrentForceVector += GraphHelperFunctions.CalcRepulsionForce(Nodes, Nodes2);
                        }
                    }
                }

                //Move all nodes and rest
                foreach (Node Nodes in AllNodes.Values)
                {
                    Nodes.MoveDelta(Nodes.cuyrrentForceVector);
                    //Nodes.myARObject.transform.position += Nodes.cuyrrentForceVector;
                    Nodes.cuyrrentForceVector = new Vector3(0, 0, 0);
                }
            }

            //Move all nodes around in 2D space x,y -> after force direct project into 3D
            //RandomMoveAllNodes(true);

            //break if we don't project into 3D space
            if (!proj3d)
                return;

            System.Random r = new System.Random((int)DateTime.Now.Ticks);

            foreach (Node Nodes in AllNodes.Values)
            {
                Nodes.MoveDelta(0, r.Next(GraphConfiguration.GRAPHBOUNDINGBOX_YMIN * 100, GraphConfiguration.GRAPHBOUNDINGBOX_YMAX * 100) / 100f, 0);
            }



        }

        //Color Changes
        public void ShowDenseNodes()
        {
            foreach (Node n in AllNodes.Values)
            {
                if (n.EdgesIn.Count > Types.GraphConfiguration.DEF_DENSE_NODES && n.EdgesOut.Count > Types.GraphConfiguration.DEF_DENSE_NODES)
                {
                    n.ChangeNodeColor(Visuals.Colors.Red);
                    foreach (Edge e in n.EdgesIn.Values)
                        e.ChangeEdgeColor(Visuals.Colors.Yellow);
                    foreach (Edge e in n.EdgesOut.Values)
                        e.ChangeEdgeColor(Visuals.Colors.Orange);
                    continue;
                }

                if (n.EdgesIn.Count > Types.GraphConfiguration.DEF_DENSE_NODES)
                {
                    n.ChangeNodeColor(Visuals.Colors.Yellow);
                    foreach (Edge e in n.EdgesIn.Values)
                        e.ChangeEdgeColor(Visuals.Colors.Yellow);
                    continue;
                }

                if (n.EdgesOut.Count > Types.GraphConfiguration.DEF_DENSE_NODES)
                {
                    n.ChangeNodeColor(Visuals.Colors.Orange);
                    foreach (Edge e in n.EdgesOut.Values)
                        e.ChangeEdgeColor(Visuals.Colors.Orange);
                    continue;
                }


                n.HideNode();
                foreach (Edge e in n.EdgesIn.Values)
                    e.HideEdge();
                foreach (Edge e in n.EdgesOut.Values)
                    e.HideEdge();

                /*n.ChangeNodeColor(Visuals.Colors.Green_P25);
                foreach (Edge e in n.EdgesIn.Values)
                    e.ChangeEdgeColor(Visuals.Colors.Blue_P25);
                foreach (Edge e in n.EdgesOut.Values)
                    e.ChangeEdgeColor(Visuals.Colors.Blue_P25);*/
            }




        }
        public void ResetColors()
        {
            foreach (Node n in AllNodes.Values)
            {

                n.ChangeNodeColor(Visuals.Colors.Blue);
                n.ShowNode();
            }

            foreach (Edge e in AllEdges.Values)
            {

                e.ChangeEdgeColor(Visuals.Colors.Green);
                e.ShowEdge();
            }

        }

        /// <summary>
        /// DEPRICATED IMPLEMENTED IN NODE MOVEMENTS
        /// </summary>
        private void correctEdgeLocations()
        {
            throw new NotSupportedException();

            foreach (Edge Edge in AllEdges.Values)
                Edge.RecenterEdges();

        }

        public void ReloadGraph(GraphTypes gt )
        {
            RaiseFeedback("Has not been implemented yet");



            //Remove all nodes and edges, wipe lists and reload
            foreach (Edge e in AllEdges.Values)
            {
                e.gameObject.SetActive(false);
                //DestroyImmediate(e.gameObject);
            }
            foreach (Node n in AllNodes.Values)
            {
                n.gameObject.SetActive(false);
                //DestroyImmediate(n.gameObject);
            }
            this.AllEdges = new Dictionary<uint, Edge>();
            this.AllNodes = new Dictionary<string, Node>();
            switch (gt)
            {
                case GraphTypes.Neo4jLocal:
                    AR.Core.Communications.Neo4jConnector.getInstance().GetGraphFromQuery(this, "MATCH p=()-[r:ORDERS]->() RETURN p LIMIT 100");
                    break;
                case GraphTypes.Simple:
                    AR.Core.Communications.GraphMLGraphFactory.GetGraphFromURL(this, AR.Core.Types.GraphConfiguration.URL_SIMPLEGRAPH);
                    break;
                case GraphTypes.Sample:
                    AR.Core.Communications.GraphMLGraphFactory.GetGraphFromURL(this, AR.Core.Types.GraphConfiguration.URL_SAMPLEGRAPH);
                    break;
                default:
                    break;
            }
        }

        //TODO implement graph traversal DFS, BFS, etc...
        private void DFSVisit(Node node)
        {
            node.isVisited = true;

            foreach (var e in node.EdgesOut)
            {
                var edge = e.Value;

                if (edge.EndNode.isVisited)
                    continue;

                edge.EndNode.isVisited = true;

                //change color slowly here
                EnumeratorRunning = true;
                StartCoroutine(ChangeNodeColorWait(edge)); //Change Color and delay a little for effect
                StartCoroutine(BlockOn()); //wait until first Coroutine stops then continue


                DFSVisit(edge.EndNode);
            }
        }
        public void BFS()
        {

            //reset all nodes to not visited
            foreach (var n in AllNodes)
            {
                n.Value.isVisited = false;
                n.Value.ChangeNodeColor(Visuals.Colors.Black);
                n.Value.HideNode();
            }
            foreach (var e in AllEdges)
            {
                e.Value.HideEdge();
                e.Value.HideEdge();
                e.Value.ChangeEdgeColor(Visuals.Colors.Black);

            }



            var node = AllNodes.First().Value;
            StartCoroutine(BreadthFirstSearchNodeVisit(node));

            /*foreach (var n in AllNodes)
            {
                if (!n.Value.isVisited)
                    StartCoroutine(DepthFirstSearchNodeVisit(n.Value));
                    //DFSVisit(n.Value);
            }*/
        }
        public IEnumerator BreadthFirstSearchNodeVisit(Node node)
        {
            node.isVisited = true;
            yield return new WaitForSeconds(2.0f);
            node.ChangeNodeColor(Visuals.Colors.Red);
            node.ShowNode();
            foreach (var e in node.EdgesOut)
            {
                var edge = e.Value;

                if (edge.EndNode.isVisited)
                    continue;

                StartCoroutine(BreadthFirstSearchNodeVisit(edge.EndNode));
                edge.ChangeEdgeColor(Visuals.Colors.Yellow);
                edge.ShowEdge();
            }
        }

        public void DFS()
        {

            //reset all nodes to not visited
            foreach (var n in AllNodes)
            {
                n.Value.isVisited = false;
                n.Value.ChangeNodeColor(Visuals.Colors.Black);
                n.Value.HideNode();
            }
            foreach (var e in AllEdges)
            {
                e.Value.HideEdge();
                e.Value.HideEdge();
                e.Value.ChangeEdgeColor(Visuals.Colors.Black);

            }
            var node = AllNodes.First().Value;
            StartCoroutine(DepthFirstSearchNodeVisit(node));
        }
        public IEnumerator DepthFirstSearchNodeVisit(Node node)
        {
            node.isVisited = true;
            yield return new WaitForSeconds(2.0f);
            node.ChangeNodeColor(Visuals.Colors.Red);
            node.ShowNode();
            foreach (var e in node.EdgesOut)
            {
                var edge = e.Value;

                if (!edge.EndNode.isVisited)
                    StartCoroutine(DepthFirstSearchNodeVisit(edge.EndNode));


                edge.ChangeEdgeColor(Visuals.Colors.Yellow);
                edge.ShowEdge();
            }
        }

        //Monobehavior methods below
        private void Awake()
        {
            myLogs.LogMessage(LoggingLevels.Verbose, "Awake Graph Method Called", Module: "Graph.Awake", Version: "ALPHA");

            //Start the speech Engine here
            mySpeechEngine = this.gameObject.AddComponent<Speech.SpeechProcessing>();
            mySpeechEngine.m_graph = this;
            myInteractibleManager = this.gameObject.AddComponent<ARTouch.InteractibleManager>(); ; //singleton access pattern
            myInteractibleManager.m_graph = this;
            myGestureManager = this.gameObject.AddComponent<ARTouch.GestureManager>(); ; //singleton access pattern
            myGestureManager.m_graph = this;


            try
            {

                myLogs.LogMessage(LoggingLevels.Verbose, "Graph Init Speach OKAY" , Module: "Graph.Awake", Version: "ALPHA");
            }
            catch (Exception exp)
            {
                myLogs.LogMessage(LoggingLevels.Verbose, "Graph Init Speach ERRPR" + exp.Message, Module: "Graph.Awake", Version: "ALPHA");
            }



           
        }

        // Use this for initialization
        void Start()
        {
            myLogs.LogMessage(LoggingLevels.Verbose, "Start Graph Method Called", Module: "Graph.Start", Version: "ALPHA");
            //RandomMoveAllNodes(false);

        }

        // Update is called once per frame
        void Update()
        {



        }

        //IEnumerator for moving and changing things

        Boolean EnumeratorRunning;
        IEnumerator ChangeNodeColorWait(Edge edge)
        {
            print("changing color " + edge.ID.ToString());
            edge.EndNode.ChangeNodeColor(Visuals.Colors.Red);
            yield return new WaitForSeconds(5.0f);
            EnumeratorRunning = false;
            print("changing color done after delay " + edge.ID.ToString());

        }
        IEnumerator BlockOn()
        {
            while (EnumeratorRunning)
                yield return new WaitForSeconds(.1f);
            print("unblocked and moving on");
        }

    }




}
