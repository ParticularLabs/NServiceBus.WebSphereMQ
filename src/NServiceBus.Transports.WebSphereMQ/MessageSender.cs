namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Linq;
    using IBM.XMS;
    using Logging;
    using Serializers.Json;

    public class MessageSender : ISendMessages
    {
        private readonly SessionFactory sessionFactory;
        private static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);
        static readonly ILog Logger = LogManager.GetLogger(typeof(MessageSender));

        public MessageSender(SessionFactory sessionFactory)
        {
            this.sessionFactory = sessionFactory;
        }

        public void Send(TransportMessage message, Address address)
        {
            ISession session = null;

            try
            {
                session = sessionFactory.GetSession();

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

                    sessionFactory.CommitSession(session);
                }
            }
            finally
            {
                sessionFactory.DisposeSession(session);
            }
        }

        static bool IsTopic(Address address)
        {
            return address.Queue.StartsWith("topic://");
        }
    }
}