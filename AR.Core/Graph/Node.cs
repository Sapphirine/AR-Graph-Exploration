using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using UnityEngine;
using ARTypes = AR.Core.Types;

namespace AR.Core.Graph
{

    public class Node : MonoBehaviour
    {
        private Logging.DBLogger myLogs;




        //Graph Properties
        private UInt32 _ID;
        public UInt32 ID
        {
            get
            {
                return _ID;
            }
            private set
            {
                _ID = value;
            }
        }
        public String UserID { get; set; }
        public Int32 Weight { get; set; }
        public String Label { get; set; }
        public Dictionary<String, System.Object> Properties { get; set; }

        public Boolean isVisited;
        public Vector3 Position { get { return myARObject.transform.position; } set { myARObject.transform.position = value; } }
        public Vector3 cuyrrentForceVector;

        public Dictionary<UInt32, Edge> EdgesIn
        {
            get
            {
                return _EdgesIn;
            }
            private set
            {
                _EdgesIn = value;
            }
        }
        private Dictionary<UInt32, Edge> _EdgesIn;
        private Dictionary<UInt32, Edge> _EdgesOut;
        public Dictionary<UInt32, Edge> EdgesOut
        {
            get
            {
                return _EdgesOut;
            }
            private set
            {
                _EdgesOut = value;
            }
        }

        public String Neo4jPath { get; set; }



        public GameObject myARObject { get; set; }


        public Node()
        {
            ID = ++Globals.ID_NodesUsed;
            UserID = ID.ToString();
            myLogs = Logging.DBLogger.getInstance();


            //properties below
            isVisited = false;

            Properties = new Dictionary<string, object>();
            EdgesOut = new Dictionary<uint, Edge>();
            EdgesIn = new Dictionary<uint, Edge>();

            myLogs.LogMessage(ARTypes.LoggingLevels.Verbose, "Start Node Method Called", Module: "Node.Start", Version: "ALPHA");
            Label = "";

        }

        // Use this for initialization
        void Start()
        {
          


        }


        private void Awake()
        {
            myLogs.LogMessage(ARTypes.LoggingLevels.Verbose, "Awake Node Method Called", Module: "Node.Awake", Version: "ALPHA");


            myARObject = Visuals.UnityHelperFunctions.CreateGameObject(Types.GraphProperties.Node,
                PrimitiveType.Sphere, Visuals.Colors.Blue);

            myARObject.transform.localScale = new Vector3(
                ARTypes.GraphConfiguration.NODE_DIAMETER, ARTypes.GraphConfiguration.NODE_DIAMETER, Types.GraphConfiguration.NODE_DIAMETER);


        }

        void GazeEntered()
        {

            myLogs.LogMessage(ARTypes.LoggingLevels.Verbose, "Node yas GazeEntered", Module: "Node.GazeEntered", Version: "ALPHA");

        }


        public Boolean isDirectConnected(Node n)
        {
            foreach (Edge ed in EdgesIn.Values)
            {
                if (ed.StartNode.ID == n.ID)
                    return true;
            }
            foreach (Edge ed in EdgesOut.Values)
            {
                if (ed.EndNode.ID == n.ID)
                    return true;
            }

            return false;   
        }

        //Movement of nodes
        public Node MoveTo(Vector3 vec)
        {
            //Instance Move below
            //myARObject.transform.position = vec;
            //CleanEdges();

            //Start a smooth movement from start to end!!!
            StartCoroutine(SmoothMoveObject(myARObject.transform.position, vec, Types.GraphConfiguration.TIME_ANIMATION_MOVENODES));

            return this;

        }

        private void Node_PropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            throw new NotImplementedException();
        }

        public Node MoveDelta(Vector3 vec)
        {
            //Instance Move below
            var curLoc = myARObject.transform.position;
            //myARObject.transform.position = new Vector3(vec.x + curLoc.x, vec.y + curLoc.y, vec.z + curLoc.z);
            //CleanEdges();

            //Start a smooth movement from start to end!!!
            var moveToVec = new Vector3(vec.x + curLoc.x, vec.y + curLoc.y, vec.z + curLoc.z);
            StartCoroutine(SmoothMoveObject(myARObject.transform.position, moveToVec, 1f));

            return this;

        }

        public Node Scale(Vector3 vec)
        {
            myARObject.transform.localScale = vec;
            return this;

        }
        public Node MoveTo(float x, float y, float z)
        {
            myLogs.LogMessage(ARTypes.LoggingLevels.Verbose, "Moving Node to x:" + x.ToString() + " y:"+y.ToString() + " z:" +z.ToString(), Module: "Node.GazeEntered", Version: "ALPHA");
            Vector3 vec = new Vector3(x, y, z);
            return MoveTo(vec);
        }
        public Node MoveDelta(float x, float y, float z)
        {
            Vector3 vec = new Vector3(x, y, z);
            return MoveDelta(vec);
        }
        public Node Scale(float x, float y, float z)
        {
            Vector3 vec = new Vector3(x, y, z);
            myARObject.transform.localScale = vec;
            return this;

        }
        public Node EvenlyScale(float evenly)
        {
            Vector3 vec = new Vector3(evenly, evenly, evenly);
            myARObject.transform.localScale = vec;
            return this;

        }
        public Node ChangeNodeColor(Visuals.Colors c)
        {
            myARObject.GetComponent<MeshRenderer>().material.color = c.myColor;
            return this;
        }
        public Node ChangeNodeColor(Color c)
        {
            myARObject.GetComponent<MeshRenderer>().material.color = c;
            return this;
        }
        public Node HideNode()
        {
            myARObject.GetComponent<MeshRenderer>().enabled = false;
            return this;
        }
        public Node ShowNode()
        {
            myARObject.GetComponent<MeshRenderer>().enabled = true;
            return this;
        }

        public Node ChangeNodeTransparency(float Transparency)
        {
            var curColor = myARObject.GetComponent<MeshRenderer>().material.color;

            curColor.a = Transparency;
            myARObject.GetComponent<MeshRenderer>().material.color = curColor;
            return this;
        }

        private void CleanEdges()
        {
            foreach (Edge Edge in EdgesIn.Values)
                Edge.RecenterEdges();
            foreach (Edge Edge in EdgesOut.Values)
                Edge.RecenterEdges();
        }

        IEnumerator SmoothMoveObject(Vector3 startPos, Vector3 endPos, float time)
        {

            float i = 0.0f;
            float rate = 1.0f / time;
            while (i < 1.0f)
            {
                i += Time.deltaTime * rate;
                myARObject.transform.position = Vector3.Lerp(startPos, endPos, i);
                CleanEdges();

                yield return null;

            }
        }  

        IEnumerator WaitforSomeSeconds(float waittime)
        {

            yield return new WaitForSeconds(waittime);

        }


    }

}
