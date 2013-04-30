namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Threading;
    using IBM.XMS;
    using Settings;

    public class ConnectionFactory : IDisposable
    {
        private readonly IConnectionFactory connectionFactory;
        private IConnection connection;
        private readonly object lockObj = new object();
        private bool disposed;

        public ConnectionFactory(WebSphereMqSettings settings)
        {
            // Create the connection factories factory
            var factoryFactory = XMSFactoryFactory.GetInstance(XMSC.CT_WMQ);

            // Use the connection factories factory to create a connection factory
            connectionFactory = factoryFactory.CreateConnectionFactory();

            // Set the properties
            connectionFactory.SetStringProperty(XMSC.WMQ_HOST_NAME, settings.Hostname);
            connectionFactory.SetIntProperty(XMSC.WMQ_PORT, settings.Port);
            connectionFactory.SetStringProperty(XMSC.WMQ_CHANNEL, settings.Channel);
            connectionFactory.SetIntProperty(XMSC.WMQ_CONNECTION_MODE, XMSC.WMQ_CM_CLIENT);
            connectionFactory.SetStringProperty(XMSC.WMQ_QUEUE_MANAGER, settings.QueueManager);
        }

        ~ConnectionFactory()
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

        public IConnection CreateNewConnection()
        {
            var newConnection = connectionFactory.CreateConnection();
            newConnection.ClientID = ClientId();

            newConnection.Start();

            return newConnection;
        }

        public IConnection GetPooledConnection()
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

                connection = connectionFactory.CreateConnection();
                connection.ClientID = ClientId();
                
                if (!SettingsHolder.Get<bool>("Endpoint.SendOnly"))
                {
                    connection.Start();
                }
            }

            return connection;
        }

        private static string ClientId()
        {
            var clientId = String.Format("NServiceBus-{0}-{1}-{2}", Address.Local, Configure.DefineEndpointVersionRetriever(),
                                         Thread.CurrentThread.ManagedThreadId);
            return clientId;
        }
    }
}