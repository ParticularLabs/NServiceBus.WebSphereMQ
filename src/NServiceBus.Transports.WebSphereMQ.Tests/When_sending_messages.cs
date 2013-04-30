namespace NServiceBus.Transports.WebSphereMQ.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Transactions;
    using NUnit.Framework;
    using Receivers;
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
}
