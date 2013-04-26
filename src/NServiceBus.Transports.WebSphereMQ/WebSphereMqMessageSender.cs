namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Transactions;
    using IBM.XMS;
    using Logging;
    using Serializers.Json;
    using Settings;


    public class WebSphereMqMessageSender : ISendMessages
    {
        private readonly WebSphereMqConnectionFactory factory;
        private readonly bool transactionsEnabled;
        private static readonly ThreadLocal<ISession> CurrentSession = new ThreadLocal<ISession>();
        private static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);
        static readonly ILog Logger = LogManager.GetLogger(typeof(WebSphereMqMessageSender));

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
        public static void SetSession(ISession session)
        {
            CurrentSession.Value = session;
        }

        public void Send(TransportMessage message, Address address)
        {
            ISession session;

            if (CurrentSession.IsValueCreated)
            {
                session = CurrentSession.Value;
            }
            else
            {
                var connection = factory.CreateConnection();

                session = connection.CreateSession(transactionsEnabled, AcknowledgeMode.AutoAcknowledge);
            }

            try
            {
                var isTopic = IsTopic(address);
                var destination = isTopic ? session.CreateTopic(address.Queue) : session.CreateQueue(address.Queue);

                using (destination)
                using (var producer = session.CreateProducer(destination))
                {
                    var mqMessage = session.CreateBytesMessage();

                    mqMessage.JMSMessageID = message.Id;

                    if (message.Body != null)
                    {
                        mqMessage.WriteBytes(message.Body);
                    }

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
                        mqMessage.JMSReplyTo = isTopic ? session.CreateTopic(message.ReplyToAddress.Queue) : session.CreateQueue(message.ReplyToAddress.Queue);
                    }
                    
                    producer.Send(mqMessage);
                    Logger.DebugFormat(isTopic ? "Message published to {0}." : "Message sent to {0}.", address.Queue);

                    if (!CurrentSession.IsValueCreated)
                    {
                        if (transactionsEnabled && Transaction.Current == null)
                        {
                            session.Commit();
                        }
                    }
                }
            }
            finally
            {
                if (!CurrentSession.IsValueCreated)
                {
                    session.Close();
                    session.Dispose();
                }
            }
        }

        static bool IsTopic(Address address)
        {
            return address.Queue.StartsWith("topic://");
        }
    }
}