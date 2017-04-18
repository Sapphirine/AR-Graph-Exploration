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
        public string[] m_Keywords = 
            {"Graph Properties", "Dense", "Reset", "Load Simple Graph", "Load Sample Graph", "Load Database Graph",
            "Depth First Search", "Breadth First Search", "Move Nodes", "Disable Speach", "Enable Speach" };
        public Graph.Graph m_graph;
        private KeywordRecognizer m_Recognizer;

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

                default:
                    break;
            }
        }

    }
}
