namespace NServiceBus.Transports.WebSphereMQ.Senders
{
    using System.Transactions;
    using IBM.XMS;

    public class NoTransactionMessageSender : MessageSender
    {
        public NoTransactionMessageSender(ConnectionFactory connectionFactory)
        {
            this.connectionFactory = connectionFactory;
        }

        protected override void InternalSend()
        {
            using (var session = connectionFactory.GetPooledConnection().CreateSession(false, AcknowledgeMode.AutoAcknowledge))
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

                session.Close();

            }
        }

        readonly ConnectionFactory connectionFactory;
    }
}