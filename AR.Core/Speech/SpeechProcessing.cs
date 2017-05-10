using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;
using UnityEngine.Windows.Speech;
using AR.Core.Types;

namespace AR.Core.Speech
{
    public class SpeechProcessing : MonoBehaviour
    {
        private Logging.DBLogger myLogs;
        public string[] m_Keywords = AR.Core.Types.SystemSetup.RootSpeechKeywords.ToArray<String>();
         
        public Graph.Graph m_graph;
        public KeywordRecognizer m_Recognizer;


        public SpeechProcessing()
        {
            myLogs = Logging.DBLogger.getInstance();
            myLogs.LogMessage(LoggingLevels.Verbose, "SpeechProcessing Constructor Called", Module: "SpeechProcessing.SpeechProcessing", Version: "ALPHA");

        }

        private void Awake()
        {
            myLogs.LogMessage(LoggingLevels.Verbose, "SpeechProcessing Awake Called", Module: "SpeechProcessing.Awake", Version: "ALPHA");

            m_Recognizer = new KeywordRecognizer(m_Keywords);
            m_Recognizer.OnPhraseRecognized += OnPhraseRecognized;
            m_Recognizer.Start();

        }

        public void Start() 
        {
            myLogs.LogMessage(LoggingLevels.Verbose, "SpeechProcessing Start Called", Module: "SpeechProcessing.Start", Version: "ALPHA");
        }

        public void resetSpecchList(List<String> newKeywords)
        {
            myLogs.LogMessage(LoggingLevels.Verbose, "Resetting Speech List: ", Module: "SpeechProcessing.resetSpecchList", Version: "ALPHA");


            var totalList = new List<String>(AR.Core.Types.SystemSetup.RootSpeechKeywords);


            if (totalList != null)
                totalList.AddRange(newKeywords);

            if (m_Recognizer.IsRunning)
                m_Recognizer.Stop();


            m_Keywords = totalList.ToArray<String>();
            m_Recognizer = new KeywordRecognizer(totalList.ToArray<String>());
            m_Recognizer.OnPhraseRecognized += OnPhraseRecognized;
            m_Recognizer.Start();


            StringBuilder sb = new StringBuilder();

            //can be used to dedupe
            HashSet<String> uniqueCommands = new HashSet<string>();


            foreach (var st in m_Keywords)
            {
                sb.Append(st + " . ");
            }

            myLogs.LogMessage(LoggingLevels.Verbose, "Resetting Speech List: " + sb.ToString(), Module: "SpeechProcessing.resetSpecchList", Version: "ALPHA");

        }

        private void OnPhraseRecognized(PhraseRecognizedEventArgs args)
        {
            myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: " + String.Format("{0} ({1})", args.text, args.confidence), Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");

            //Do Some work
            switch (args.text)
            {
                case "Dense":
                    m_graph.RaiseFeedback("Showing highly connected nodes");
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Dense (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.ShowDenseNodes();
                    break;
                case "Depth First Search":
                    m_graph.RaiseFeedback("Visualizing Depth First Search");
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Depth First Search (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.DFS();
                    break;
                case "Breadth First Search":
                    m_graph.RaiseFeedback("Visualizing breadth First Search");
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Breadth First Search(Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.BFS();
                    break;
                case "Stop":
                    m_graph.RaiseFeedback(" Stopping ");
                    break;
                case "Graph Properties":
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Graph Properties (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.RaiseFeedback(AR.Core.Voice.VoiceManager.RelayGraphProperties(m_graph));
                    break;
                case "Reset":
                    m_graph.RaiseFeedback("Reseting graph properties");
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Reset (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.ResetColors();
                    break;
                case "Load Simple Graph":
                    m_graph.RaiseFeedback("Loading Simple Graph");
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Load Simple Graph (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.ReloadGraph(GraphTypes.Simple);
                    break;
                case "Load Sample Graph":
                    m_graph.RaiseFeedback("Loading Sample Graph");
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Load Sample Graph(Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.ReloadGraph(GraphTypes.Sample);
                    break;
                case "Load Database Graph":
                    m_graph.RaiseFeedback("Loading Graph from Neo 4 Jay");
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Load Database Graph (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.ReloadGraph(GraphTypes.Neo4jLocal);
                    break;
                case "Move Nodes":
                    m_graph.RaiseFeedback("Moving Nodes");
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Moving Nodes (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.RandomMoveAllNodes(false);
                    break;
                case "Disable Speach":
                    m_graph.RaiseFeedback("Disabling Speach");
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Disable Speach (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.removeSpeach = true;
                    break;
                case "Enable Speach":
                    m_graph.removeSpeach = false;
                    m_graph.RaiseFeedback("Enabling Speach");
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Enable Speach (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    break;
                case "Show Edges":
                    m_graph.RaiseFeedback("Showing All Edges");
                    m_graph.ShowAllEdges();
                    break;
                case "Hide Edges":
                    m_graph.RaiseFeedback("Hiding All Edges");
                    m_graph.HideAllEdges();
                    break;
                case "Show Nodes":
                    m_graph.RaiseFeedback("Showing All Nodes");
                    m_graph.ShowAllNodes();
                    break;
                case "Hide Nodes":
                    m_graph.RaiseFeedback("Hiding All Nodes");
                    m_graph.HideAllNodes();
                    break;
                case "List Commands":
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: List Commands (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.RaiseFeedback("Listing Commands ");
                    StringBuilder sb = new StringBuilder();

                    //can be used to dedupe
                    HashSet<String> uniqueCommands = new HashSet<string>();


                    foreach (var st in m_Keywords)
                    {
                        sb.Append(st + " . ");
                    }
                    m_graph.RaiseFeedback("Commands Available. " +  sb.ToString());
                    myLogs.LogMessage(LoggingLevels.Verbose, "Commands Available:" + sb.ToString(), Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    break;
                default:
                    //handle the approximate matches here
                    if (args.text.Contains("Node Scale Color"))
                    {
                        var propname = args.text.Replace("Node Scale Color", "").Trim();
                        m_graph.RaiseFeedback(args.text); //say what you're doing here
                        m_graph.ColorNodesPropBased(propname);
                    }
                    if (args.text.Contains("Node Scale Size"))
                    {
                        var propname = args.text.Replace("Node Scale Size", "").Trim();
                        m_graph.RaiseFeedback(args.text); //say what you're doing here
                        m_graph.ScaleNodesPropBased(propname);
                    }
                    if (args.text.Contains("Edge Scale Color"))
                    {
                        var propname = args.text.Replace("Edge Scale Color", "").Trim();
                        m_graph.RaiseFeedback(args.text); //say what you're doing here
                        m_graph.ColorEdgePropBased(propname);
                    }
                    if (args.text.Contains("Edge Scale Size"))
                    {
                        var propname = args.text.Replace("Edge Scale Size", "").Trim();

                        m_graph.RaiseFeedback(args.text + ", Not Implemented Correctly"); //say what you're doing here
                        //m_graph.ScaleEdgePropBased(propname);
                    }

                    break;
            }
        }

    }
}
