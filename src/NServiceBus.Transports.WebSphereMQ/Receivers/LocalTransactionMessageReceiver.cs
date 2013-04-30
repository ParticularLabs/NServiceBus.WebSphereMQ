namespace NServiceBus.Transports.WebSphereMQ.Receivers
{
    using System;
    using IBM.XMS;

    public class LocalTransactionMessageReceiver : MessageReceiver
    {
        protected override void Run(IMessageConsumer consumer, ISession session, ReceiveResult result)
        {
            IMessage message = consumer.ReceiveNoWait();

            if (message == null)
            {
                return;
            }

            result.MessageId = message.JMSMessageID;

            bool success = false;
            Exception exceptionRaised = null;

            try
            {
                success = ProcessMessage(message);
            }
            catch (Exception ex)
            {
                exceptionRaised = ex;
            }

            if (success)
            {
                session.Commit();
            }
            else
            {
                session.Rollback();
            }

            if (exceptionRaised != null)
            {
                throw exceptionRaised;
            }
        }
    }
}