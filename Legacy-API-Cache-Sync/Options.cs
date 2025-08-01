using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Legacy_API_Cache_Sync
{
    using Newtonsoft.Json.Linq;

    public class Options
    {
        public static string ApiUrl { get; set; }
        
        public static string RestUrl { get; set; }

        public static string ApiKey { get; set; }

        public static int ApiFreq { get; set; }

        public static string jsonPath { get; set; }

        public static string serviceId { get; set; }

        public static JArray Endpoints { get; set; }

        public static string MySqlHost { get; set; }
        public static string MySqlUsername { get; set; }
        public static string MySqlPassword { get; set; }
        public static string MySqlDatabase { get; set; }

        public static string MdtServer { get; set; }
    }
}
