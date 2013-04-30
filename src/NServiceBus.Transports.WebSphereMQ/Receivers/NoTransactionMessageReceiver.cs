namespace NServiceBus.Transports.WebSphereMQ.Receivers
{
    using IBM.XMS;

    public class NoTransactionMessageReceiver : MessageReceiver
    {
        protected override void Run(IMessageConsumer consumer, ISession session, ReceiveResult result)
        {
            IMessage message = consumer.ReceiveNoWait();
            if (message == null)
            {
                return;
            }

            result.MessageId = message.JMSMessageID;

            ProcessMessage(message);
        }
    }
}