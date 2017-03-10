using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace AR.Core.Graph
{
    public class UnityVisualProperties : IUnityVisualProperties
    {
        public Color myColor { get; set; }
        public  Mesh myMesh { get; set; }
        public Material myMaterial { get; set; }
        public Vector3 myLocation { get; set; }
        public Vector3 myRotation { get; set; }
        public Vector3 myScale { get; set; }
        public float mySize { get; set; }
    }

    public interface  IUnityVisualProperties
    {
        Color myColor { get; set; }
        Mesh myMesh { get; set; }
        Material myMaterial { get; set; }
        Vector3 myLocation { get; set; }
        Vector3 myRotation { get; set; }
        Vector3 myScale { get; set; }
        float mySize { get; set; }
    }

}
