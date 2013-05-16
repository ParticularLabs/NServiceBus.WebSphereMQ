namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Collections.Generic;


    public class MessagePublisher : IPublishMessages
    {
        private readonly ISendMessages messageSender;

        public MessagePublisher(ISendMessages messageSender)
        {
            this.messageSender = messageSender;
        }

        public bool Publish(TransportMessage message, IEnumerable<Type> eventTypes)
        {
            var eventTypeTopics = String.Join("/", eventTypes);

            messageSender.Send(message, Address.Parse(String.Format("topic://{0}/EVENTS/{1}", WebSphereMqAddress.GetQueueName(Address.Local), eventTypeTopics)));

            return true;
        }
    }
}