namespace NServiceBus.Transports.WebSphereMQ.Receivers
{
    using System;
    using System.Threading;
    using IBM.XMS;
    using Utils;

    public class NoTransactionMessageReceiver : MessageReceiver
    {
        protected override void Receive(CancellationToken token, IConnection connection)
        {
            var backOff = new BackOff(MaximumDelay);

            while (!token.IsCancellationRequested)
            {
                IMessage message;

                using (ISession session = connection.CreateSession(false, AcknowledgeMode.AutoAcknowledge))
                {
                    using (IMessageConsumer consumer = createConsumer(session))
                    {
                        message = consumer.ReceiveNoWait();

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
                }

                backOff.Wait(() => message == null);
            }
        }
    }
}