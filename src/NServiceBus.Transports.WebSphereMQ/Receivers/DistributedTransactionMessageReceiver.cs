namespace NServiceBus.Transports.WebSphereMQ.Receivers
{
    using System;
    using System.Threading;
    using System.Transactions;
    using IBM.XMS;
    using Utils;

    public class DistributedTransactionMessageReceiver : MessageReceiver
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
                        IMessage message;

                        using (var lockReset = new ManualResetEventSlim(false))
                        {
                            using (var scope = new TransactionScope(TransactionScopeOption.Required, transactionOptions))
                            {
                                Transaction.Current.TransactionCompleted += (sender, args) => lockReset.Set();

                                message = consumer.ReceiveNoWait();

                                if (message != null)
                                {
                                    Exception exception = null;
                                    try
                                    {
                                        if (ProcessMessage(message))
                                        {
                                            scope.Complete();
                                        }
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

                            lockReset.Wait();
                        }

                        backOff.Wait(() => message == null);
                    }

                    consumer.Close();
                }

                session.Close();
            }
        }
    }
}