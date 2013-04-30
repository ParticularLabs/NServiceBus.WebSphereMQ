namespace NServiceBus.Transports.WebSphereMQ.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Transactions;
    using NUnit.Framework;
    using Settings;
    using TransactionSettings = Unicast.Transport.TransactionSettings;

    [TestFixture]
    [Explicit]
    public class When_publishing_messages
    {
        [SetUp]
        public void Init()
        {
            SettingsHolder.Set("Endpoint.SendOnly", false);
            SettingsHolder.Set("Transactions.Enabled", true);

            Address.InitializeLocalAddress("Foo");
        }

        [Test]
        public void Should_receive_with_transactions()
        {
            SettingsHolder.Set("Transactions.Enabled", true);
            SettingsHolder.Set("Transactions.DefaultTimeout", TimeSpan.FromSeconds(30));
            SettingsHolder.Set("Transactions.IsolationLevel", IsolationLevel.ReadCommitted);
            SettingsHolder.Set("Transactions.SuppressDistributedTransactions", false);
            SettingsHolder.Set("Transactions.DoNotWrapHandlersExecutionInATransactionScope", false);

            ManualResetEvent manualResetEvent = new ManualResetEvent(false);

            var settings = new WebSphereMqSettings();
            var webSphereMqConnectionFactory =
                new ConnectionFactory(settings);

            TransportMessage tm = new TransportMessage();
            tm.Body = Encoding.UTF8.GetBytes("Hello John");
            tm.CorrelationId = "oi there";
            tm.TimeToBeReceived = TimeSpan.FromMinutes(10);
            tm.Recoverable = true;
            tm.Headers.Add(Headers.ContentType, "text/xml");

            var subscriptionsManager = new SubscriptionsManager(webSphereMqConnectionFactory);
            subscriptionsManager.Init(new TransactionSettings(), message =>
                {
                    Console.Out.WriteLine("Event received, with correlationid={0}", message.CorrelationId);
                    return true;
                }, (s, exception) =>
                {
                    Console.Out.WriteLine("EndProcessMessage");
                    manualResetEvent.Set();
                });

            subscriptionsManager.Subscribe(typeof(MyType), Address.Local);

            subscriptionsManager.Start(1);

            MessagePublisher publisher = new MessagePublisher(new MessageSender(new SessionFactory(webSphereMqConnectionFactory)));

            using (TransactionScope scope = new TransactionScope(TransactionScopeOption.Required, TimeSpan.FromSeconds(30)))
            {
                publisher.Publish(tm, new List<Type> {typeof (MyType)});
                scope.Complete();
            }

            manualResetEvent.WaitOne();
            subscriptionsManager.Stop();
        }

        public class MyType { }

    }

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
            MessageSender sender = new MessageSender(new SessionFactory(new ConnectionFactory(new WebSphereMqSettings { Port=1415, Channel = "NewOne", QueueManager = "Test2" })));
            sender.Send(tm, Address.Parse("Boo"));
        }

        [Test]
        public void Should_send_with_transactions()
        {
            var webSphereMqConnectionFactory =
                new ConnectionFactory(new WebSphereMqSettings { Port = 1415, Channel = "NewOne", QueueManager = "Test2" });

            TransportMessage tm = new TransportMessage();
            tm.Body = Encoding.UTF8.GetBytes("Hello John");
            tm.CorrelationId = "oi there";
            tm.TimeToBeReceived = TimeSpan.FromMinutes(10);
            tm.Recoverable = true;
            tm.Headers.Add(Headers.ContentType, "text/xml");

            using (TransactionScope scope = new TransactionScope(TransactionScopeOption.Required,
                                                              TimeSpan.FromSeconds(30)))
            {
                MessageSender sender = new MessageSender(new SessionFactory(webSphereMqConnectionFactory));
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


            var sphereMqSettings = new WebSphereMqSettings
                {
                    Port = 1415,
                    Channel = "NewOne",
                    QueueManager = "Test2"
                };
            var webSphereMqConnectionFactory = new ConnectionFactory(sphereMqSettings);

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

            var webSphereMqSessionFactory = new SessionFactory(webSphereMqConnectionFactory);
            MessageSender sender = new MessageSender(webSphereMqSessionFactory);
            DequeueStrategy dequeuer = new DequeueStrategy(new SubscriptionsManager(webSphereMqConnectionFactory), new MessageReceiver(webSphereMqSessionFactory, webSphereMqConnectionFactory));

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
