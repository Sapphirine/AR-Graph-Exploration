using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;
using AR.Core.Types;

namespace AR.Core.Graph
{
    public static class GraphHelperFunctions
    {
        public static Vector3 CalcRepulsionForce(Node x, Node y)
        {
            var proximity = (x.Position - y.Position).magnitude;
            double force = -(GraphConfiguration.FORCE_DIRECTED_REPULSIVE_CONST / Math.Pow(proximity, 2));
            Vector3 vec = (x.Position - y.Position);
            vec.Normalize();
            return (Convert.ToSingle(force) * vec);

        }
        public static Vector3 CalcAttractionForce(Node x, Node y, double springLength)
        {
            var proximity = (x.Position - y.Position).magnitude;
            double force = GraphConfiguration.FORCE_DIRECTED_ATTRACTIVE_CONST * Math.Max(proximity - springLength, 0);
            Vector3 vec = (x.Position - y.Position);
            vec.Normalize();
            return (Convert.ToSingle(force) * vec);
        }

    }
}
