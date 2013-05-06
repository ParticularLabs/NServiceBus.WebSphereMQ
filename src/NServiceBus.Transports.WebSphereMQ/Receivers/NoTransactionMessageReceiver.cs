namespace NServiceBus.Transports.WebSphereMQ.Receivers
{
    using System;
    using System.Threading;
    using IBM.XMS;

    public class NoTransactionMessageReceiver : MessageReceiver
    {
        IMessageConsumer consumer;

        protected override void Receive(CancellationToken token, IConnection connection)
        {
            token.Register(() =>
                {
                    if (consumer != null)
                    {
                        consumer.Close();
                    }
                });
            
            while (!token.IsCancellationRequested)
            {
                using (ISession session = connection.CreateSession(false, AcknowledgeMode.AutoAcknowledge))
                {
                    using (consumer = createConsumer(session))
                    {
                        IMessage message = consumer.Receive();

                        if (message != null)
                        {
                            Exception exception = null;

                            try
                            {
                                ProcessMessage(message);
                            }
                            catch (Exception ex)
                            {
                                Logger.Error("Error processing message.", ex);

                                exception = ex;
                            }
                            finally
                            {
                                endProcessMessage(message.JMSMessageID, exception);
                            }
                        }
                    }

                    session.Close();
                }
            }
        }
    }
}