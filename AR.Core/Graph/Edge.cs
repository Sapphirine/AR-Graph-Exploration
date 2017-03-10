using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace AR.Core.Graph
{
    public class Edge 
    {

        public UInt32 ID { get; set; }
        public float Weight { get; set; }
        public String Label { get; set; }
        public Dictionary<String, System.Object> Properties { get; set; }
        public Node StartNode { get; set; }
        public Node EndNode { get; set; }

        public GameObject myARObject { get; set; }

        public Edge(GameObject myARObject, Vector3 vStart, Vector3 vEnd)
        {
            ID = Globals.ID_EdgesUsed;
            Globals.ID_EdgesUsed++;
            Properties = new Dictionary<string, object>();
            
            //position edge
            myARObject.transform.position = (vEnd - vStart) / 2.0f + vStart;
            //scale edge
            var v3T = myARObject.transform.localScale;
            v3T.x = v3T.z = Types.GraphConfiguration.EDGE_WIDTH;
            v3T.y = (vStart - vEnd).magnitude;
            myARObject.transform.localScale = v3T;
            //rotate edge
            myARObject.transform.rotation = Quaternion.FromToRotation(Vector3.up, vStart - vEnd);

            this.myARObject = myARObject;
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
    }
}
