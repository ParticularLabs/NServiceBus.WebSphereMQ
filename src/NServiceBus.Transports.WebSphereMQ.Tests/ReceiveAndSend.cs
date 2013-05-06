namespace NServiceBus.Transports.WebSphereMQ.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Schedulers;
    using System.Transactions;
    using IBM.XMS;
    using NUnit.Framework;

    [TestFixture]
    public class ReceiveAndSend
    {
        [Test]
        public void ReceiveSend3()
        {
            CreateInitialMessage();

            IMessage message;
            using (var connection = CreateConnection())
            {
                using (ISession session = connection.CreateSession(false, AcknowledgeMode.AutoAcknowledge))
                {
                    using (IMessageConsumer consumer = session.CreateConsumer(session.CreateQueue("MyClient")))
                    {
                        message = consumer.ReceiveNoWait();
                        if (message == null)
                        {
                            Console.Out.WriteLine("No Message!");
                            return;
                        }

                    }
                }
            
                using (var scope = new TransactionScope(TransactionScopeOption.Required))
                using (ISession session2 = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
                {
                    using (var producer = session2.CreateProducer(session2.CreateQueue("audit")))
                    {

                        //Transaction.Current.
                        //var mqMessage = session.CreateTextMessage("Hello john");

                        producer.Send(message);
                    }
                    scope.Complete();
                }

            }

        }

        [Test]
        public void ReceiveSend2Multi()
        {
            List<Task> tasks = new List<Task>();
            CancellationTokenSource source = new CancellationTokenSource();
            var mtaTaskScheduler = new MTATaskScheduler(30, "dsfsd");

            for (int i = 0; i < 30; i++)
            {
                tasks.Add(
                Task.Factory.StartNew(()=>ReceiveSend2(source.Token), CancellationToken.None, TaskCreationOptions.None, mtaTaskScheduler));
            }

            Thread.Sleep(3000);
            source.Cancel();

            Task.WaitAll(tasks.ToArray());

        }

        [Test]
        public void ReceiveSend2(CancellationToken token)
        {
            //CreateInitialMessage();
            IMessageConsumer consumer = null;

            token.Register(() =>
                {
                    if (consumer != null)
                    {
                        Console.Out.WriteLine("Stopping consumer...");
                        consumer.Close();
                        Console.Out.WriteLine("Consumer stopped");

                    }
                });

            using (var connection = CreateConnection())
            {
                using (ISession session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
                {
                    using (consumer = session.CreateConsumer(session.CreateQueue("PerformanceTest")))
                    {
                        Console.Out.WriteLine("Receiving...");
                        IMessage message = consumer.Receive();
                        if (message == null)
                        {
                            Console.Out.WriteLine("No Message!");
                            return;
                        }
                    }

                    session.Commit();
                }
            }

            Console.Out.WriteLine("All done");
        }


        private static void CreateInitialMessage()
        {
            using (var connection = CreateConnection())
            {
                using (ISession session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
                {
                    using (var producer = session.CreateProducer(session.CreateQueue("MyClient")))
                    {
                        var mqMessage = session.CreateTextMessage("Hello john2");

                        producer.Send(mqMessage);
                    }
                    session.Commit();
                }

            }
        }

        private static IConnection CreateConnection()
        {
            // Create the connection factories factory
            var factoryFactory = XMSFactoryFactory.GetInstance(XMSC.CT_WMQ);

            // Use the connection factories factory to create a connection factory
            var cf = factoryFactory.CreateConnectionFactory();

            // Set the properties
            cf.SetStringProperty(XMSC.WMQ_HOST_NAME, "localhost");
            cf.SetIntProperty(XMSC.WMQ_PORT, 1415);
            cf.SetStringProperty(XMSC.WMQ_CHANNEL, "NewOne");
            cf.SetIntProperty(XMSC.WMQ_CONNECTION_MODE, XMSC.WMQ_CM_CLIENT);
            cf.SetStringProperty(XMSC.WMQ_QUEUE_MANAGER, "Test2");

            var clientId = String.Format("NServiceBus-{0}", Thread.CurrentThread.ManagedThreadId);
            cf.SetStringProperty(XMSC.CLIENT_ID, clientId);

            var connection = cf.CreateConnection();

            connection.Start();

            return connection;
        }
    }
}
