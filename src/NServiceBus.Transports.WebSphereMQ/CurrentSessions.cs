namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Threading;
    using IBM.XMS;

    public class CurrentSessions : IDisposable
    {
        private readonly ThreadLocal<ISession> sessionsPerThread = new ThreadLocal<ISession>();
        private bool disposed;

        /// <summary>
        ///     Sets the native session.
        /// </summary>
        /// <param name="session">
        ///     Native <see cref="ISession" />.
        /// </param>
        public void SetSession(ISession session)
        {
            sessionsPerThread.Value = session;
        }

        public ISession GetSession()
        {
            if (sessionsPerThread.IsValueCreated)
            {
                return sessionsPerThread.Value;                    
            }

            return null;
        }
        
        ~CurrentSessions()
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
                sessionsPerThread.Dispose();
            }

            disposed = true;
        }
    }
}