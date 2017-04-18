using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AR.Core.Types
{
    public class TextArgs : EventArgs
    {
        private string szMessage;
        public TextArgs(string TextMessage)
        {
            szMessage = TextMessage;
        }

        public string Message
        {
            get { return szMessage; }
            set { szMessage = value; }
        }
    }
}
