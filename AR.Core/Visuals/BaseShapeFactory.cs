using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace AR.Core.Visuals
{
    public static class BaseShapeFactory
    {
        public static GameObject CreateBaseShape(PrimitiveType t)
        {
            return GameObject.CreatePrimitive(t);
        }

    }
}
