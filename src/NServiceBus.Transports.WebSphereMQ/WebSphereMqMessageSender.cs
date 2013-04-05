namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Transactions;
    using IBM.XMS;
    using Serializers.Json;
    using Settings;


    public class WebSphereMqMessageSender : ISendMessages
    {
        private readonly WebSphereMqConnectionFactory factory;
        private readonly bool transactionsEnabled;
        private readonly ThreadLocal<ISession> currentSession = new ThreadLocal<ISession>();
        private static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);

        public WebSphereMqMessageSender(WebSphereMqConnectionFactory factory)
        {
            this.factory = factory;
            transactionsEnabled = SettingsHolder.Get<bool>("Transactions.Enabled");
        }

        /// <summary>
        ///     Sets the native session.
        /// </summary>
        /// <param name="session">
        ///     Native <see cref="ISession" />.
        /// </param>
        public void SetSession(ISession session)
        {
            currentSession.Value = session;
        }

        public void Send(TransportMessage message, Address address)
        {
            var connection = factory.CreateConnection();

            ISession session;

            if (currentSession.IsValueCreated)
            {
                session = currentSession.Value;
            }
            else
            {
                session = connection.CreateSession(transactionsEnabled && (Transaction.Current != null),
                                                   transactionsEnabled
                                                       ? AcknowledgeMode.AutoAcknowledge
                                                       : AcknowledgeMode.DupsOkAcknowledge);
            }

            try
            {
                var destination = session.CreateQueue(address.Queue);

                using (destination)
                using (var producer = session.CreateProducer(destination))
                {
                    var mqMessage = session.CreateBytesMessage();
                    mqMessage.WriteBytes(message.Body);
                    mqMessage.JMSMessageID = message.Id;

                    if (!string.IsNullOrEmpty(message.CorrelationId))
                    {
                        mqMessage.JMSCorrelationID = message.CorrelationId;
                    }

                    producer.DeliveryMode = message.Recoverable
                                                ? DeliveryMode.Persistent
                                                : DeliveryMode.NonPersistent;

                    if (message.TimeToBeReceived < TimeSpan.MaxValue)
                    {
                        producer.TimeToLive = (long) message.TimeToBeReceived.TotalMilliseconds;
                    }

                    mqMessage.SetStringProperty(Constants.NSB_HEADERS, Serializer.SerializeObject(message.Headers));

                    if (message.Headers.ContainsKey(Headers.EnclosedMessageTypes))
                    {
                        mqMessage.JMSType =
                            message.Headers[Headers.EnclosedMessageTypes]
                                .Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries)
                                .FirstOrDefault();
                    }

                    if (message.Headers.ContainsKey(Headers.ContentType))
                    {
                        mqMessage.SetStringProperty(XMSC.JMS_IBM_FORMAT, message.Headers[Headers.ContentType]);
                    }

                    if (message.ReplyToAddress != null && message.ReplyToAddress != Address.Undefined)
                    {
                        mqMessage.JMSReplyTo = session.CreateQueue(message.ReplyToAddress.Queue);
                    }

                    producer.Send(mqMessage);
                }
            }
            finally
            {
                if (!currentSession.IsValueCreated)
                {
                    session.Close();
                    session.Dispose();
                }
            }
        }
    }
}