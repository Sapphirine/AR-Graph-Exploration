using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AR.Core.Types
{
    public static class GraphConfiguration
    {
 
        public static BoundryScale plotScale = BoundryScale.Normal;
        public static float scaleFactor
        {
            get
            {
                switch (plotScale)
                {
                    case (BoundryScale.Mini):
                        return BoundryScale_Mini;
                    case (BoundryScale.Normal):
                        return BoundryScale_Normal;
                    case (BoundryScale.Room):
                        return BoundryScale_Room;
                    default:
                        return 1f;


                }
            }
        }

        //bounding box scaling factors
        private static float BoundryScale_Mini = .01f;
        private static float BoundryScale_Normal = .1f;
        private static float BoundryScale_Room = 1f;

        //Bounding Box with scaling factors set above
        public static float EDGE_WIDTH = .025f * scaleFactor;
        public static float NODE_DIAMETER = .25f * scaleFactor;
        public static Int32 GRAPHBOUNDINGBOX_XMIN = Convert.ToInt32(-10f * scaleFactor);
        public static Int32 GRAPHBOUNDINGBOX_XMAX = Convert.ToInt32( 10f * scaleFactor);
        public static Int32 GRAPHBOUNDINGBOX_YMIN = Convert.ToInt32(0f * scaleFactor);
        public static Int32 GRAPHBOUNDINGBOX_YMAX = Convert.ToInt32(10f * scaleFactor);
        public static Int32 GRAPHBOUNDINGBOX_ZMIN = Convert.ToInt32(-10f * scaleFactor);
        public static Int32 GRAPHBOUNDINGBOX_ZMAX = Convert.ToInt32(10f * scaleFactor);

        //Force directed graph properties
        public static float FORCE_DIRECTED_REPULSIVE_CONST = 10000;
        public static float FORCE_DIRECTED_ATTRACTIVE_CONST = .1f;
        public static float FORCE_DIRECTED_SPRING_LEN = 100;
        public static Int32 FORCE_DIRECTED_ITERATIONS = 75;
        public static float FORCE_DIRECTED_DEFAULT_DAMPING = 0.5f;
        public static Boolean EXTRACT_SAMPLEGRAPHS = true;


        public static float TIME_ANIMATION_MOVENODES = 3.0f;

        public static Int32 DEF_DENSE_NODES = 1;



        public static String URL_SAMPLEGRAPH = "http://awesome.cs.jhu.edu/data/static/graphs/cat/mixed.species_brain_1.graphml";
        public static String URL_SIMPLEGRAPH = "http://graphml.graphdrawing.org/primer/simple.graphml";



    }

}
