namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using IBM.XMS;
    using Settings;

    public class WebSphereMqConnectionFactory : IDisposable
    {
        private readonly WebSphereMqSettings settings;
        private IConnection connection;
        private readonly object lockObj = new object();
        private bool disposed;

        public WebSphereMqConnectionFactory(WebSphereMqSettings settings)
        {
            this.settings = settings;
        }

        ~WebSphereMqConnectionFactory()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (disposed)
            {
                return;
            }

            if (disposing)
            {
                if (connection != null)
                {
                    connection.Stop();
                    connection.Close();
                    connection.Dispose();
                }
            }

            disposed = true;
        }

        public IConnection CreateConnection()
        {
            if (connection != null)
            {
                return connection;
            }

            lock (lockObj)
            {
                if (connection != null)
                {
                    return connection;
                }

                // Create the connection factories factory
                var factoryFactory = XMSFactoryFactory.GetInstance(XMSC.CT_WMQ);

                // Use the connection factories factory to create a connection factory
                var cf = factoryFactory.CreateConnectionFactory();

                // Set the properties
                cf.SetStringProperty(XMSC.WMQ_HOST_NAME, settings.Hostname);
                cf.SetIntProperty(XMSC.WMQ_PORT, settings.Port);
                cf.SetStringProperty(XMSC.WMQ_CHANNEL, settings.Channel);
                cf.SetIntProperty(XMSC.WMQ_CONNECTION_MODE, XMSC.WMQ_CM_CLIENT);
                cf.SetStringProperty(XMSC.WMQ_QUEUE_MANAGER, settings.QueueManager);
                //cf.SetIntProperty(XMSC.WMQ_CLIENT_RECONNECT_OPTIONS, XMSC.WMQ_CLIENT_RECONNECT);

                var clientId = String.Format("NServiceBus-{0}-{1}", Address.Local, Configure.DefineEndpointVersionRetriever());
                cf.SetStringProperty(XMSC.CLIENT_ID, clientId);

                connection = cf.CreateConnection();
                
                if (!SettingsHolder.Get<bool>("Endpoint.SendOnly"))
                {
                    connection.Start();
                }
            }

            return connection;
        }
    }
}