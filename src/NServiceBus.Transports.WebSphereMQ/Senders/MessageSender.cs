namespace NServiceBus.Transports.WebSphereMQ.Senders
{
    using System;
    using System.Linq;
    using IBM.XMS;
    using Serializers.Json;

    public abstract class MessageSender : ISendMessages
    {
        public void Send(TransportMessage message, Address address)
        {
            messageToSend = message;
            destinationAddress = new WebSphereMqAddress(address);
            InternalSend();
        }

        protected abstract void InternalSend();

        protected IBytesMessage CreateNativeMessage(ISession session, IMessageProducer producer)
        {
            var mqMessage = session.CreateBytesMessage();

            mqMessage.JMSMessageID = messageToSend.Id;

            if (messageToSend.Body != null)
            {
                mqMessage.WriteBytes(messageToSend.Body);
            }

            if (!string.IsNullOrEmpty(messageToSend.CorrelationId))
            {
                mqMessage.JMSCorrelationID = messageToSend.CorrelationId;
            }

            producer.DeliveryMode = messageToSend.Recoverable
                                        ? DeliveryMode.Persistent
                                        : DeliveryMode.NonPersistent;

            if (messageToSend.TimeToBeReceived < TimeSpan.MaxValue)
            {
                producer.TimeToLive = (long)messageToSend.TimeToBeReceived.TotalMilliseconds;
            }

            mqMessage.SetStringProperty(Constants.NSB_HEADERS, Serializer.SerializeObject(messageToSend.Headers));

            if (messageToSend.Headers.ContainsKey(Headers.EnclosedMessageTypes))
            {
                mqMessage.JMSType =
                    messageToSend.Headers[Headers.EnclosedMessageTypes]
                        .Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries)
                        .FirstOrDefault();
            }

            if (messageToSend.Headers.ContainsKey(Headers.ContentType))
            {
                mqMessage.SetStringProperty(XMSC.JMS_IBM_FORMAT, messageToSend.Headers[Headers.ContentType]);
            }


            if (messageToSend.ReplyToAddress != null && messageToSend.ReplyToAddress != Address.Undefined)
            {
                var replyToAddress = new WebSphereMqAddress(messageToSend.ReplyToAddress);

                mqMessage.JMSReplyTo = replyToAddress.CreateDestination(session);
            }
            return mqMessage;
        }

        protected WebSphereMqAddress destinationAddress;

        TransportMessage messageToSend;


        static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);

    }
}