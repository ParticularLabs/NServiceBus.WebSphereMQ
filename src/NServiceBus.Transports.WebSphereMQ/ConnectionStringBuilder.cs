namespace NServiceBus.Transports.WebSphereMQ
{
    using System.Data.Common;

    class ConnectionStringBuilder : DbConnectionStringBuilder
    {
        public ConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public WebSphereMqSettings RetrieveSettings()
        {
            var settings = new WebSphereMqSettings();

            if (ContainsKey("hostname"))
                settings.Hostname = (string)this["hostname"];

            if (ContainsKey("port"))
                settings.Port = int.Parse((string)this["port"]);

            if (ContainsKey("channel"))
                settings.Channel = (string)this["channel"];

            if (ContainsKey("queueManager"))
                settings.QueueManager = (string)this["queueManager"];


            return settings;
        }
    }
}