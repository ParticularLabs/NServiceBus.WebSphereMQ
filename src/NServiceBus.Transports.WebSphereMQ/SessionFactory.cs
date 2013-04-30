namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Threading;
    using System.Transactions;
    using IBM.XMS;
    using TransactionSettings = Unicast.Transport.TransactionSettings;

    public class SessionFactory : IDisposable
    {
        private readonly ThreadLocal<ISession> currentSession = new ThreadLocal<ISession>();
        private readonly ConnectionFactory connectionFactory;
        private bool disposed;
        private readonly TransactionSettings transactionSettings;
        private readonly bool reuseSession;

        public SessionFactory(ConnectionFactory connectionFactory)
        {
            this.connectionFactory = connectionFactory;

            transactionSettings = new TransactionSettings();
            if (transactionSettings.IsTransactional)
            {
                reuseSession = true;
            }
            else
            {
                reuseSession = transactionSettings.DoNotWrapHandlersExecutionInATransactionScope;
            }
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
                if (reuseSession)
                {
                    return currentSession.Value;                    
                }
            }

            // Send only session
            return connectionFactory.GetPooledConnection().CreateSession(true, AcknowledgeMode.AutoAcknowledge);
        }

        public void CommitSession(ISession session)
        {
            if (!(reuseSession && currentSession.IsValueCreated) && Transaction.Current == null)
            {
                session.Commit();
            }
        }

        public void DisposeSession(ISession session)
        {
            if (!(reuseSession && currentSession.IsValueCreated))
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