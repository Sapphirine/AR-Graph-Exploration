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
        public string[] m_Keywords = { "Dense", "Sparce", "Test", "Badger", "Reset" };
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
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Updating the Badger Nodes (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.ShowDenseNodes();
                    break;
                case "Reset":
                    myLogs.LogMessage(LoggingLevels.Verbose, "OnPhraseRecognized: Updating the Badger Nodes (Test)", Module: "SpeechProcessing.OnPhraseRecognized", Version: "ALPHA");
                    m_graph.ResetColors();
                    break;

                default:
                    break;

            }
        }


    }
}
