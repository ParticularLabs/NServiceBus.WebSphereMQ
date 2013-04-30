namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Threading;
    using System.Transactions;
    using IBM.XMS;
    using Settings;

    public class SessionFactory : IDisposable
    {
        private readonly ThreadLocal<ISession> currentSession = new ThreadLocal<ISession>();
        private readonly ConnectionFactory factory;
        private readonly bool transactionsEnabled;
        private bool disposed;

        public SessionFactory(ConnectionFactory factory)
        {
            this.factory = factory;
            transactionsEnabled = SettingsHolder.Get<bool>("Transactions.Enabled");
        }

        /// <summary>
        ///     Sets the native session.
        /// </summary>
        /// <param name="session">
        ///     Native <see cref="ISession" />.
        /// </param>
        public void SetSession(ISession session)
        {
            currentSession.Value = session;
        }

        public ISession GetSession()
        {
            if (currentSession.IsValueCreated)
            {
                return currentSession.Value;
            }

            var connection = factory.GetPooledConnection();

            return connection.CreateSession(transactionsEnabled, AcknowledgeMode.AutoAcknowledge);
        }

        public void CommitSession(ISession session)
        {
            if (transactionsEnabled && !currentSession.IsValueCreated && Transaction.Current == null)
            {
                session.Commit();
            }
        }

        public void DisposeSession(ISession session)
        {
            if (!currentSession.IsValueCreated)
            {
                session.Dispose();
            }
        }

        ~SessionFactory()
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
                currentSession.Dispose();
            }

            disposed = true;
        }
    }
}