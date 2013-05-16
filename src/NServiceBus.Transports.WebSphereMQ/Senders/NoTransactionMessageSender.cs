namespace NServiceBus.Transports.WebSphereMQ.Senders
{
    using System.Transactions;
    
    public class NoTransactionMessageSender : MessageSender
    {
        protected override void InternalSend()
        {
            using (var session = CreateSession())
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
    }
}