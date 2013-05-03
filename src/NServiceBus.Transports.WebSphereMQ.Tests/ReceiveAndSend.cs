namespace NServiceBus.Transports.WebSphereMQ.Tests
{
    using System;
    using System.Threading;
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
        public void ReceiveSend2()
        {
            CreateInitialMessage();

            IMessage message;
            using (var connection = CreateConnection())
            {
                using (ISession session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
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

                    session.Commit();

                    using (var scope = new TransactionScope(TransactionScopeOption.Required))
                    {
                        using (var producer = session.CreateProducer(session.CreateQueue("audit")))
                        {

                            //Transaction.Current.
                            //var mqMessage = session.CreateTextMessage("Hello john");

                            producer.Send(message);
                        }
                        scope.Complete();
                    }

                }
            }

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
