namespace NServiceBus.Transports.WebSphereMQ.Senders
{
    using System.Transactions;

    public class DistributedTransactionMessageSender : MessageSender
    {
        public CurrentSessions CurrentSessions { get; set; }

        protected override void InternalSend()
        {
            var hasExistingSession = true;
            var session = CurrentSessions.GetSession();

            if (session == null)
            {
                hasExistingSession = false;
                session = CreateSession();
            }

            try
            {
                using (var destination = destinationAddress.CreateDestination(session))
                using (var producer = session.CreateProducer(destination))
                {
                    var mqMessage = CreateNativeMessage(session, producer);

                    if (!hasExistingSession)
                    {
                        using (new TransactionScope(TransactionScopeOption.Suppress))
                        {
                            producer.Send(mqMessage);
                        }
                    }
                    else
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

        
    }
}