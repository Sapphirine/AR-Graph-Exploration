/****************************** Module Header ******************************\
Module Name:  AR.CORE
Project:      3D Graph Rendering on Microsoft Hololens

//This is boilerplate code from Miccrosoft and not create by Sam Guleff
//Attribute this code to Microsoft Acadamy!!
//https://developer.microsoft.com/en-us/windows/mixed-reality/holograms_210
\***************************************************************************/
using UnityEngine;
using UnityEngine.VR.WSA.Input;
using AR.Core.Logging;

//Attribute this code to Microsoft Acadamy!!


namespace AR.Core.Graph.ARTouch
{
    /// <summary>
    /// GestureManager contains event handlers for subscribed gestures.
    /// </summary>
    public class GestureManager : MonoBehaviour
    {
        private GestureRecognizer gestureRecognizer;
        public Graph m_graph;
        private DBLogger myLogs;

        public GestureManager()
        {
            myLogs = AR.Core.Logging.DBLogger.getInstance();
            myLogs.LogMessage(AR.Core.Types.LoggingLevels.Verbose, "Starting GestureManager", "GestureManager.Constructor", "Alpha");
        }

        void Start()
        {
            gestureRecognizer = new GestureRecognizer();
            gestureRecognizer.SetRecognizableGestures(GestureSettings.Tap);

            gestureRecognizer.TappedEvent += (source, tapCount, ray) =>
            {
                GameObject focusedObject = InteractibleManager.Instance.FocusedGameObject;

                if (focusedObject != null)
                {
                    myLogs.LogMessage(AR.Core.Types.LoggingLevels.Verbose, "GestureManager clicked", "gestureRecognizer.TappedEvent", "Alpha");

                    if (m_graph != null)
                    {
                        var test = "";
                        var GOName = focusedObject.name;
                        if (GOName.Contains("Node"))
                        {


                            Node node = m_graph.AllNodes[GOName.Split('_')[1]];

                            myLogs.LogMessage(AR.Core.Types.LoggingLevels.Verbose, "Looking up Node info: " + node.UserID, "InteractibleManager.update", "Alpha");


                            string Message = AR.Core.Voice.VoiceManager.RelayNodeProperties(node);
                            m_graph.RaiseFeedback(Message);
                        }
                        if (GOName.Contains("Edge"))
                        {

                            Edge edge = m_graph.AllEdges[System.Convert.ToUInt16(GOName.Split('_')[1])];
                            string Message = AR.Core.Voice.VoiceManager.RelayEdgeProperties(edge);
                            m_graph.RaiseFeedback(Message);
                        }
                        else
                            test = "NOT FOUND";

                        myLogs.LogMessage(AR.Core.Types.LoggingLevels.Verbose, "Object Name: " + GOName + " / Found Object: " + test, "InteractibleManager.update", "Alpha");


                    }
                    else
                    {
                        //Intentionally blank no graph discovered (Have we init'ed yet)?
                        myLogs.LogMessage(AR.Core.Types.LoggingLevels.Verbose, "Where's my Graph At??", "InteractibleManager.update", "Alpha");


                    }
                }
            };

            gestureRecognizer.StartCapturingGestures();
        }


        void OnDestroy()
        {
            gestureRecognizer.StopCapturingGestures();
        }
    }
}