namespace NServiceBus.Transports.WebSphereMQ.Receivers
{
    using System.Threading;
    using System.Transactions;
    using IBM.XMS;

    public class DistributedTransactionMessageReceiver : MessageReceiver
    {
        protected override void Run(IMessageConsumer consumer, ISession session, ReceiveResult result)
        {
            using (var lockReset = new ManualResetEventSlim(false))
            {
                using (var scope = new TransactionScope(TransactionScopeOption.Required, transactionOptions))
                {
                    IMessage message = consumer.ReceiveNoWait();

                    if (message == null)
                    {
                        return;
                    }

                    result.MessageId = message.JMSMessageID;

                    Transaction.Current.TransactionCompleted += (sender, args) => lockReset.Set();

                    if (ProcessMessage(message))
                    {
                        scope.Complete();
                    }
                }

                lockReset.Wait();
            }
        }
    }
}