using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AR.Core.Types
{
    public static class GraphConfiguration
    {
        public static float EDGE_WIDTH = .025f;
        public static float NODE_DIAMETER = .25f;
        public static Int32 GRAPHBOUNDINGBOX_XMIN = -10;
        public static Int32 GRAPHBOUNDINGBOX_XMAX = 10;
        public static Int32 GRAPHBOUNDINGBOX_YMIN = 0;
        public static Int32 GRAPHBOUNDINGBOX_YMAX = 10;
        public static Int32 GRAPHBOUNDINGBOX_ZMIN = -10;
        public static Int32 GRAPHBOUNDINGBOX_ZMAX = 10;
        public static float FORCE_DIRECTED_REPULSIVE_CONST = 10000;
        public static float FORCE_DIRECTED_ATTRACTIVE_CONST = .1f;
        public static float FORCE_DIRECTED_SPRING_LEN = 100;
        public static Int32 FORCE_DIRECTED_ITERATIONS = 75;
        public static float FORCE_DIRECTED_DEFAULT_DAMPING = 0.5f;
        public static Boolean EXTRACT_SAMPLEGRAPHS = true;


        public static String URL_SAMPLEGRAPH = "http://awesome.cs.jhu.edu/data/static/graphs/cat/mixed.species_brain_1.graphml";
        public static String URL_SIMPLEGRAPH = "http://graphml.graphdrawing.org/primer/simple.graphml";



    }
}
