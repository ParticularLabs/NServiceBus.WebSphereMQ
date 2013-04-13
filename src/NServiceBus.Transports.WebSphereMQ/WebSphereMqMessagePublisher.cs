namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Collections.Generic;


    public class WebSphereMqMessagePublisher : IPublishMessages
    {
        private readonly ISendMessages messageSender;

        public WebSphereMqMessagePublisher(ISendMessages messageSender)
        {
            this.messageSender = messageSender;
        }

        public bool Publish(TransportMessage message, IEnumerable<Type> eventTypes)
        {
            var eventTypeTopics = String.Join("/", eventTypes);

            messageSender.Send(message, Address.Parse(String.Format("topic://{0}/EVENTS/{1}", Address.Local.Queue, eventTypeTopics)));

            return true;
        }
    }
}