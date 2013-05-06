namespace NServiceBus.Transports.WebSphereMQ.Receivers
{
    using System;
    using System.Threading;
    using IBM.XMS;

    public class LocalTransactionMessageReceiver : MessageReceiver
    {
        private IMessageConsumer consumer;

        protected override void Receive(CancellationToken token, IConnection connection)
        {
            using (ISession session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
            {
                CurrentSessions.SetSession(session);

                token.Register(() =>
                {
                    if (consumer != null)
                    {
                        consumer.Close();
                    }
                });

                using (consumer = createConsumer(session))
                {
                    while (!token.IsCancellationRequested)
                    {
                        IMessage message = consumer.Receive();

                        if (message != null)
                        {
                            Exception exception = null;
                            try
                            {
                                if (ProcessMessage(message))
                                {
                                    session.Commit();
                                }
                                else
                                {
                                    session.Rollback();
                                }
                            }
                            catch (Exception ex)
                            {
                                Logger.Error("Error processing message.", ex);

                                session.Rollback();

                                exception = ex;
                            }
                            finally
                            {
                                endProcessMessage(message.JMSMessageID, exception);
                            }
                        }
                    }
                }
                session.Close();
            }
        }
    }
}