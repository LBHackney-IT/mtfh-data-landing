using MTFHDataLanding.Boundary;

namespace MTFHDataLanding.Helpers
{
    public static class Strings
    {
        public static string GenerateFileName(EntityEventSns message, string keyName, string year, string month, string day)
        {
            return keyName + "year=" + year + "/month=" + month + "/day=" + day + "/" +
                   message.DateTime.ToString("HH\\:mm\\:ss.fffffff") + ".json";
        }
    }
}
