using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace AR.Core.Types
{
    public class ForceVectorPair
    {
        public double force;
        public Vector3 vec;

        public ForceVectorPair(double force, Vector3 vec)
        {
            this.force = force;
            this.vec = vec;

        }

    }
}
