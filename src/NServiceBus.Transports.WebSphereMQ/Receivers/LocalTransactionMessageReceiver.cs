namespace NServiceBus.Transports.WebSphereMQ.Receivers
{
    using System;
    using System.Threading;
    using IBM.XMS;
    using Utils;

    public class LocalTransactionMessageReceiver : MessageReceiver
    {
        protected override void Receive(CancellationToken token, IConnection connection)
        {
            var backOff = new BackOff(MaximumDelay);

            using (ISession session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
            {
                CurrentSessions.SetSession(session);

                using (IMessageConsumer consumer = createConsumer(session))
                {
                    while (!token.IsCancellationRequested)
                    {
                        IMessage message = consumer.ReceiveNoWait();

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

                        backOff.Wait(() => message == null);
                    }
                }
            }
        }
    }
}