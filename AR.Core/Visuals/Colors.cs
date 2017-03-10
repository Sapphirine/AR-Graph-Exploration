using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace AR.Core.Visuals
{
    public class Colors
    {
        public Color myColor { get; set; }

        public Colors(float r, float g, float b, float a)
        {
            myColor = new Color(r, g, b, a);
        }
        public Colors(Color c)
        {
            myColor = c;
        }



        public static Color Red = new Color(1f, 0f, 0f, 0f);
        public static Color Orange = new Color(1f, .5f, 0f, 0f);
        public static Color Yellow = new Color(1f, 1f, 0f, 0f);
        public static Color Green = new Color(0f, 1f, 0f, 0f);
        public static Color Blue = new Color(0f, 0f, 1f, 0f);
        public static Color Black = new Color(0f, 0f, 0f, 0f);
        public static Color White = new Color(1f, 1f, 1f, 0f);
        public static Color Grey = new Color(.5f, .5f, .5f, 0f);
        public static Color Transparent = new Color(0f, 0f, 0f, 1f);
    }
}
