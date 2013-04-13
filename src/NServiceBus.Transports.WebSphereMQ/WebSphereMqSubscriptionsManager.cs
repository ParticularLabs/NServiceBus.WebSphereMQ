namespace NServiceBus.Transports.WebSphereMQ
{
    using System;

    public class WebSphereMqSubscriptionsManager : IManageSubscriptions
    {
        public event EventHandler<SubscriptionEventArgs> Subscribed;
        public event EventHandler<SubscriptionEventArgs> Unsubscribed;

        public void Subscribe(Type eventType, Address publisherAddress)
        {
            if (Subscribed != null)
            {
                Subscribed(this, new SubscriptionEventArgs(eventType, publisherAddress));
            }
        }

        public void Unsubscribe(Type eventType, Address publisherAddress)
        {
            if (Unsubscribed != null)
            {
                Unsubscribed(this, new SubscriptionEventArgs(eventType, publisherAddress));
            }
        }
    }

    public class SubscriptionEventArgs : EventArgs
    {
        public Type EventType { get; set; }
        public Address PublisherAddress { get; set; }

        public SubscriptionEventArgs(Type eventType, Address publisherAddress)
        {
            EventType = eventType;
            PublisherAddress = publisherAddress;
        }
    }
}