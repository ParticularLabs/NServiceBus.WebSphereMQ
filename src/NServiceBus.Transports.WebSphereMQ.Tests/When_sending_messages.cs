namespace NServiceBus.Transports.WebSphereMQ.Tests
{
    using System;
    using System.Text;
    using System.Transactions;
    using NUnit.Framework;
    using Settings;

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
            tm.Body = Encoding.UTF8.GetBytes("Hello John");
            tm.CorrelationId = "oi there";
            tm.TimeToBeReceived = TimeSpan.FromMinutes(10);
            tm.Recoverable = true;
            tm.Headers.Add(Headers.ContentType, "text/xml");
            WebSphereMqMessageSender sender = new WebSphereMqMessageSender(new WebSphereMqConnectionFactory(new WebSphereMqSettings { QueueManager = "QM_TEST" }));
            sender.Send(tm, Address.Parse("QM_TEST.LOCAL.ONE"));
        }

        [Test]
        public void Should_send_with_transactions()
        {
            var webSphereMqConnectionFactory =
                new WebSphereMqConnectionFactory(new WebSphereMqSettings { Channel = "QM_TEST.SVRCONN", QueueManager = "QM_TEST" });

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
                sender.Send(tm, Address.Parse("QM_TEST.LOCAL.ONE"));
                scope.Complete();
            }
        }
    }
}
