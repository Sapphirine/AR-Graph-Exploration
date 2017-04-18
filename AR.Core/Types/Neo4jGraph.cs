using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace AR.Core.Types
{

    [Serializable]
    public class Neo4jGraph_SimpleJson
    {
        public String[] columns;
        public data[][] data;
    }

    public class Neo4jGraph
    {
        public String[] columns;
        public data[,] data;
    }


    [Serializable]
    public class data
    {

        public String[] nodes;
        public String[] directions;
        public String[] relationships;
        public int length;
        public String start;
        public String end;

    }

}
