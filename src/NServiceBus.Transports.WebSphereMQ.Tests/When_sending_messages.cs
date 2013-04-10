namespace NServiceBus.Transports.WebSphereMQ.Tests
{
    using System;
    using System.Text;
    using System.Threading;
    using System.Transactions;
    using NUnit.Framework;
    using Settings;
    using Unicast.Transport;
    using TransactionSettings = Unicast.Transport.TransactionSettings;

    [TestFixture]
    [Explicit]
    public class When_sending_messages
    {
        [SetUp]
        public void Init()
        {
            SettingsHolder.Set("Endpoint.SendOnly", false);
            SettingsHolder.Set("Transactions.Enabled", true);
        }

        [Test]
        public void Should_send_without_transactions()
        {
            SettingsHolder.Set("Transactions.Enabled", false);
            
            TransportMessage tm = new TransportMessage();
            tm.Id = "ID:414d51205465737432202020202020204552635120040f99";
            tm.Body = Encoding.UTF8.GetBytes("Hello John");
            tm.CorrelationId = "oi there";
            tm.TimeToBeReceived = TimeSpan.FromMinutes(10);
            tm.Recoverable = true;
            tm.Headers.Add(Headers.ContentType, "text/xml");
            WebSphereMqMessageSender sender = new WebSphereMqMessageSender(new WebSphereMqConnectionFactory(new WebSphereMqSettings { Port=1415, Channel = "NewOne", QueueManager = "Test2" }));
            sender.Send(tm, Address.Parse("Boo"));
        }

        [Test]
        public void Should_send_with_transactions()
        {
            var webSphereMqConnectionFactory =
                new WebSphereMqConnectionFactory(new WebSphereMqSettings { Port = 1415, Channel = "NewOne", QueueManager = "Test2" });

            TransportMessage tm = new TransportMessage();
            tm.Body = Encoding.UTF8.GetBytes("Hello John");
            tm.CorrelationId = "oi there";
            tm.TimeToBeReceived = TimeSpan.FromMinutes(10);
            tm.Recoverable = true;
            tm.Headers.Add(Headers.ContentType, "text/xml");

            using (TransactionScope scope = new TransactionScope(TransactionScopeOption.Required,
                                                              TimeSpan.FromSeconds(30)))
            {
                WebSphereMqMessageSender sender = new WebSphereMqMessageSender(webSphereMqConnectionFactory);
                sender.Send(tm, Address.Parse("Boo"));
                scope.Complete();
            }
        }

        [Test]
        public void Should_send_and_receive_with_transactions()
        {
            SettingsHolder.Set("Transactions.Enabled", true);
            SettingsHolder.Set("Transactions.DefaultTimeout", TimeSpan.FromSeconds(30));
            SettingsHolder.Set("Transactions.IsolationLevel", IsolationLevel.ReadCommitted);
            SettingsHolder.Set("Transactions.SuppressDistributedTransactions", false);
            SettingsHolder.Set("Transactions.DoNotWrapHandlersExecutionInATransactionScope", false);


            var webSphereMqConnectionFactory =
                new WebSphereMqConnectionFactory(new WebSphereMqSettings
                    {
                        Port = 1415,
                        Channel = "NewOne",
                        QueueManager = "Test2"
                    });

            TransportMessage tm = new TransportMessage();
            tm.Body = Encoding.UTF8.GetBytes("Hello John");
            tm.CorrelationId = "oi there";
            tm.TimeToBeReceived = TimeSpan.FromMinutes(10);
            tm.Recoverable = true;
            tm.Headers.Add(Headers.ContentType, "text/xml");

            TransportMessage tm2 = new TransportMessage();
            tm2.Id = "ID:414d51205465737432202020202020204552635120040f99";
            tm2.Body = Encoding.UTF8.GetBytes("Hello John");
            tm2.CorrelationId = "oi there";
            tm2.TimeToBeReceived = TimeSpan.FromMinutes(10);
            tm2.Recoverable = true;
            tm2.Headers.Add(Headers.ContentType, "text/xml");

            WebSphereMqDequeueStrategy dequeuer = new WebSphereMqDequeueStrategy(webSphereMqConnectionFactory);
            WebSphereMqMessageSender sender = new WebSphereMqMessageSender(webSphereMqConnectionFactory);
            dequeuer.MessageSender = sender;

            var address = Address.Parse("Boo");
            ManualResetEvent manualResetEvent = new ManualResetEvent(false);
            dequeuer.Init(address, new TransactionSettings {IsTransactional = true,}, r =>
                {
                    Console.Out.WriteLine("Message Received With Id={0}", r.Id);
                    sender.Send(tm, address);
                    sender.Send(tm, address);

                    manualResetEvent.Set();
                    return false;
                }, (s, exception) => { });


            sender.Send(tm2, address);
            
            dequeuer.Start(1);

            manualResetEvent.WaitOne();

            dequeuer.Stop();

        }
    }
}
