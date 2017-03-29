using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;
using ARTypes = AR.Core.Types;

namespace AR.Core.Graph
{
    public class Edge : MonoBehaviour
    {
        private Logging.DBLogger myLogs;
        public UInt32 ID { get; set; }
        public float Weight { get; set; }
        public String Label { get; set; }
        public Dictionary<String, System.Object> Properties { get; set; }
        public Node StartNode { get; set; }
        public Node EndNode { get; set; }

        public GameObject myARObject { get; set; }

        public Edge()
        {
            myLogs = Logging.DBLogger.getInstance();
            ID = Globals.ID_EdgesUsed;
            Globals.ID_EdgesUsed++;
          
        }

        public Edge RecenterEdges()
        {

            //get the edge locations
            Vector3 vStart = StartNode.myARObject.transform.position;
            Vector3 vEnd = EndNode.myARObject.transform.position;

            //position edge
            myARObject.transform.position = (vEnd - vStart) / 2.0f + vStart;
            //scale edge
            var v3T = myARObject.transform.localScale;
            v3T.y = v3T.x = v3T.z = Types.GraphConfiguration.EDGE_WIDTH;
            v3T.y = (vStart - vEnd).magnitude;
            myARObject.transform.localScale = v3T;
            //rotate edge
            myARObject.transform.rotation = Quaternion.FromToRotation(Vector3.up, vStart - vEnd);

            return this;

        }

        public Edge ChangeEdgeColor(Visuals.Colors c)
        {
            myARObject.GetComponent<MeshRenderer>().material.color = c.myColor;
            return this;
        }
        public Edge ChangeEdgeColor(Color c)
        {
            myARObject.GetComponent<MeshRenderer>().material.color = c;
            return this;
        }
        public Edge ChangeEdgeTransparency(float Transparency)
        {
            var curColor = myARObject.GetComponent<MeshRenderer>().material.color;
            curColor.a = Transparency;
            myARObject.GetComponent<MeshRenderer>().material.color = curColor;
            myLogs.LogMessage(ARTypes.LoggingLevels.Verbose, "Changed Transparency to (a):" + myARObject.GetComponent<MeshRenderer>().material.color.a.ToString(), Module: "Edge.ChangeEdgeTransparency", Version: "ALPHA");

            return this;
        }


        private void Awake()
        {
            myLogs.LogMessage(ARTypes.LoggingLevels.Verbose, "Awake Edge Method Called", Module: "Edge.Awake", Version: "ALPHA");

            Properties = new Dictionary<string, object>();

            myARObject = Visuals.UnityHelperFunctions.CreateGameObject(Types.GraphProperties.Edge,
               PrimitiveType.Cube, Visuals.Colors.Green);


            //get the edge locations
            Vector3 vStart = StartNode.myARObject.transform.position;
            Vector3 vEnd = EndNode.myARObject.transform.position;


            //position edge
            myARObject.transform.position = (vEnd - vStart) / 2.0f + vStart;
            //scale edge
            var v3T = myARObject.transform.localScale;
            v3T.x = v3T.z = Types.GraphConfiguration.EDGE_WIDTH;
            v3T.y = (vStart - vEnd).magnitude;
            myARObject.transform.localScale = v3T;
            //rotate edge
            myARObject.transform.rotation = Quaternion.FromToRotation(Vector3.up, vStart - vEnd);



        }

        private void Start()
        {
           

        }

        private void Update()
        {

        }

    }
}
