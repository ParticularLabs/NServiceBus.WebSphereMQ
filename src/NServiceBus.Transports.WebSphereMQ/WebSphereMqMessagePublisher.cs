namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class WebSphereMqMessagePublisher : IPublishMessages
    {
        private readonly ISendMessages messageSender;

        public WebSphereMqMessagePublisher(ISendMessages messageSender)
        {
            this.messageSender = messageSender;
        }

        public bool Publish(TransportMessage message, IEnumerable<Type> eventTypes)
        {
            var eventType = eventTypes.First(); //we route on the first event for now

            var topic = TopicNameCreator.GetName(eventType);

            messageSender.Send(message, Address.Parse(String.Format("topic://{0}", topic)));

            return true;
        }
    }
}