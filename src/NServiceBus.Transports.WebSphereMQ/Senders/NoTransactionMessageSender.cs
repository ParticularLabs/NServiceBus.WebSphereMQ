﻿namespace NServiceBus.Transports.WebSphereMQ.Senders
{
    using System;
    using System.Linq;
    using System.Transactions;
    using IBM.XMS;
    using Logging;
    using Serializers.Json;

    public class NoTransactionMessageSender : ISendMessages
    {
        private readonly ConnectionFactory connectionFactory;
        private static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);
        static readonly ILog Logger = LogManager.GetLogger(typeof(NoTransactionMessageSender));

        public NoTransactionMessageSender(ConnectionFactory connectionFactory)
        {
            this.connectionFactory = connectionFactory;
        }

        public void Send(TransportMessage message, Address address)
        {
            using (ISession session = connectionFactory.GetPooledConnection().CreateSession(false, AcknowledgeMode.AutoAcknowledge))
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
                        producer.TimeToLive = (long)message.TimeToBeReceived.TotalMilliseconds;
                    }

                    mqMessage.SetStringProperty(Constants.NSB_HEADERS, Serializer.SerializeObject(message.Headers));

                    if (message.Headers.ContainsKey(Headers.EnclosedMessageTypes))
                    {
                        mqMessage.JMSType =
                            message.Headers[Headers.EnclosedMessageTypes]
                                .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
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

                    using (new TransactionScope(TransactionScopeOption.Suppress))
                    {
                        producer.Send(mqMessage);
                    }

                    session.Close();

                    Logger.DebugFormat(isTopic ? "Message published to {0}." : "Message sent to {0}.", address.Queue);
                }
            }
        }

        static bool IsTopic(Address address)
        {
            return address.Queue.StartsWith("topic://");
        }
    }
}