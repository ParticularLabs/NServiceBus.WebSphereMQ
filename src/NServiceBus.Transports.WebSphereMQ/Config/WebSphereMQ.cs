namespace NServiceBus
{
    using Transports;

    public class WebSphereMQ : TransportDefinition
    {
        public WebSphereMQ()
        {
            HasNativePubSubSupport = true;
            HasSupportForCentralizedPubSub = true;
        }
    }
}