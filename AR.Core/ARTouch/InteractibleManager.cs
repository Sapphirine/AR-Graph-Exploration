using AR.Core.Graph.ARTouch;
using UnityEngine;
using AR.Core.Logging;

//Attribute this code to Microsoft Acadamy!!
/****************************** Module Header ******************************\
Module Name:  AR.CORE
Project:      3D Graph Rendering on Microsoft Hololens

//This is boilerplate code from Miccrosoft and not create by Sam Guleff
//Attribute this code to Microsoft Acadamy!!
//https://developer.microsoft.com/en-us/windows/mixed-reality/holograms_210
\***************************************************************************/
/// <summary>
/// InteractibleManager keeps tracks of which GameObject
/// is currently in focus.
/// </summary>
/// 

namespace AR.Core.Graph.ARTouch
{
    public class InteractibleManager : Singleton<InteractibleManager>
    {
        private DBLogger myLogs;
        public Graph m_graph;


        public GameObject FocusedGameObject { get; private set; }

        private GameObject oldFocusedGameObject = null;


        private void Awake()
        {
           
        }


        void Start()
        {
            myLogs = AR.Core.Logging.DBLogger.getInstance();
            myLogs.LogMessage(AR.Core.Types.LoggingLevels.Verbose, "Starting InteractibleManager", "InteractibleManager.Start", "Alpha");
            FocusedGameObject = null;
        }

        void Update()
        {
            oldFocusedGameObject = FocusedGameObject;

            if (GazeManager.Instance.Hit)
            {
                myLogs.LogMessage(AR.Core.Types.LoggingLevels.Verbose, "InteractibleManager.Instance.Hit", "InteractibleManager.update", "Alpha");



                RaycastHit hitInfo = GazeManager.Instance.HitInfo;
                if (hitInfo.collider != null)
                {
                    // 2.c: Assign the hitInfo's collider gameObject to the FocusedGameObject.
                    FocusedGameObject = hitInfo.collider.gameObject;
                }
                else
                {
                    FocusedGameObject = null;
                }
            }
            else
            {
                FocusedGameObject = null;
            }

            if (FocusedGameObject != oldFocusedGameObject)
            {
                ResetFocusedInteractible();
                myLogs.LogMessage(AR.Core.Types.LoggingLevels.Verbose, "InteractibleManager.Instance.Hit (NEW OBJECT)", "InteractibleManager.update", "Alpha");

                if (m_graph != null)
                {
                    return;
                    var test = "";
                    var GOName = FocusedGameObject.name;
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


                if (FocusedGameObject != null)
                {
                    if (FocusedGameObject.GetComponent<Interactible>() != null)
                    {
                        // 2.c: Send a GazeEntered message to the FocusedGameObject.
                        FocusedGameObject.SendMessage("GazeEntered");
                    }
                }
            }
        }

        void GazeEntered()
        {

            myLogs.LogMessage(AR.Core.Types.LoggingLevels.Verbose, "Graph has GazeEntered", Module: "Graph.GazeEntered", Version: "ALPHA");

        }

        private void ResetFocusedInteractible()
        {
            if (oldFocusedGameObject != null)
            {
                if (oldFocusedGameObject.GetComponent<Interactible>() != null)
                {
                    // 2.c: Send a GazeExited message to the oldFocusedGameObject.
                    oldFocusedGameObject.SendMessage("GazeExited");
                }
            }
        }
    }
}