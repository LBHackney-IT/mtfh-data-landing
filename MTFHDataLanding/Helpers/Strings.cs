using System;
using System.Collections.Generic;
using System.Text;
using MTFHDataLanding.Boundary;

namespace MTFHDataLanding.Helpers
{
    public static class Strings
    {
        public static string GenerateFileName(EntityEventSns message, string year, string month, string day)
        {
            return Constants.KEY_NAME + "year=" + year + "/month=" + month + "/day=" + day + "/" +
                   message.DateTime.ToString("HH\\:mm\\:ss.fffffff") + ".json";
        }
    }
}
