/****************************** Module Header ******************************\
Module Name:  AR.CORE
Project:      3D Graph Rendering on Microsoft Hololens

//This is boilerplate code from Miccrosoft and not create by Sam Guleff
//Attribute this code to Microsoft Acadamy!!
//https://developer.microsoft.com/en-us/windows/mixed-reality/holograms_210
\***************************************************************************/
using UnityEngine;




namespace AR.Core.Graph.ARTouch
{
    public class Singleton<T> : MonoBehaviour where T : Singleton<T>
    {
        private static T _Instance;
        public static T Instance
        {
            get
            {
                if (_Instance == null)
                {
                    _Instance = FindObjectOfType<T>();
                }
                return _Instance;
            }
        }
    }
}