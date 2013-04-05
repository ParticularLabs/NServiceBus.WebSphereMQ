namespace NServiceBus.Transports.WebSphereMQ
{
    using IBM.XMS;

    public class WebSphereMqSettings
    {
        public WebSphereMqSettings()
        {
            QueueManager = string.Empty;
            Channel = "SYSTEM.DEF.SVRCONN";
            Port = XMSC.WMQ_DEFAULT_CLIENT_PORT;
            Hostname = "localhost";
        }

        public string QueueManager { get; set; }

        public string Channel { get; set; }

        public int Port { get; set; }

        public string Hostname { get; set; }
    }
}