namespace NServiceBus.Transports.WebSphereMQ.Senders
{
    using System.Transactions;
    using IBM.XMS;

    public class LocalTransactionMessageSender : MessageSender
    {
        public LocalTransactionMessageSender(ConnectionFactory connectionFactory)
        {
            this.connectionFactory = connectionFactory;
        }

        public CurrentSessions CurrentSessions { get; set; }

        protected override void InternalSend()
        {
            var hasExistingSession = true;
            var session = CurrentSessions.GetSession();

            if (session == null)
            {
                hasExistingSession = false;
                session = connectionFactory.GetPooledConnection().CreateSession(false, AcknowledgeMode.AutoAcknowledge);
            }

            try
            {

                using (var destination = destinationAddress.CreateDestination(session))
                using (var producer = session.CreateProducer(destination))
                {
                    var mqMessage = CreateNativeMessage(session, producer);

                    using (new TransactionScope(TransactionScopeOption.Suppress))
                    {
                        producer.Send(mqMessage);
                    }

                    producer.Close();
                }
            }
            finally
            {
                if (!hasExistingSession)
                {
                    session.Close();
                    session.Dispose();
                }
            }
        }

        readonly ConnectionFactory connectionFactory;
    }
}