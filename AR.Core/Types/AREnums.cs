using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AR.Core.Types
{
    public enum GraphProperties {Node, Edge };

    public enum LoggingLevels { Verbose, Warning, Error, Critical };

    public enum BoundryScale { Mini, Normal, Room};

    public enum GraphTypes { Simple, Sample, Neo4jLocal };
}
