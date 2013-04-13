namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using Unicast.Transport;

    public class SubscriptionsConsumer
    {
        private readonly BlockingCollection<Tuple<Type, Address>> events =
            new BlockingCollection<Tuple<Type, Address>>();

        private readonly List<EventConsumerSatellite> satellites = new List<EventConsumerSatellite>();

        public WebSphereMqConnectionFactory Factory { get; set; }

        public Func<TransportMessage, bool> TryProcessMessage { get; set; }

        public Action<string, Exception> EndProcessMessage { get; set; }

        public TransactionSettings TransactionSettings { get; set; }

        /// <summary>
        /// Settings
        /// </summary>
        public WebSphereMqSettings Settings { get; set; }

        /// <summary>
        /// Message sender
        /// </summary>
        public WebSphereMqMessageSender MessageSender { get; set; }

        public SubscriptionsConsumer(WebSphereMqSubscriptionsManager subscriptionsManager)
        {
            subscriptionsManager.Subscribed +=
                (sender, e) => events.TryAdd(Tuple.Create(e.EventType, e.PublisherAddress));

            subscriptionsManager.Unsubscribed += (sender, e) =>
                {
                    var consumerSatellite = satellites.Find(consumer => consumer.InputAddress == e.PublisherAddress && consumer.EventType == e.EventType.FullName);

                    if (consumerSatellite == null)
                    {
                        return;
                    }

                    consumerSatellite.Stop();
                    satellites.Remove(consumerSatellite); 
                };
        }

        public void Start(int maximumConcurrencyLevel)
        {
            var thread = new Thread(() =>
                {
                    foreach (var tuple in events.GetConsumingEnumerable())
                    {
                        var dequeueMessages = new WebSphereMqDequeueStrategy(Factory, null);
                        var address = Address.Parse(String.Format("topic://{0}/EVENTS/#/{1}/#", tuple.Item2.Queue, tuple.Item1.FullName));
                        var eventConsumerSatellite = new EventConsumerSatellite(dequeueMessages, address, tuple.Item1.FullName);

                        dequeueMessages.SetCreateMessageConsumer((session) => session.CreateDurableSubscriber(session.CreateTopic(address.Queue), tuple.Item1.FullName));
                        dequeueMessages.Settings = Settings;
                        dequeueMessages.MessageSender = MessageSender;
                        dequeueMessages.Init(address, TransactionSettings, TryProcessMessage, EndProcessMessage);

                        satellites.Add(eventConsumerSatellite);
                        
                        eventConsumerSatellite.Start();
                    }
                }) {IsBackground = true};

            thread.SetApartmentState(ApartmentState.MTA);
            thread.Name = "WebSphereMq Subscriptions Consumer";
            thread.Start();
        }

        public void Stop()
        {
            events.CompleteAdding();
        }

        private class EventConsumerSatellite
        {
            private readonly IDequeueMessages dequeuer;
            private readonly string eventType;
            private readonly Address address;

            public EventConsumerSatellite(IDequeueMessages dequeuer, Address address, string eventType)
            {
                this.dequeuer = dequeuer;
                this.eventType = eventType;

                this.address = address;
            }

            public void Start()
            {
                dequeuer.Start(1);
            }

            public void Stop()
            {
                dequeuer.Stop();
            }

            public Address InputAddress
            {
                get { return address; }
            }

            public string EventType
            {
                get { return eventType; }
            }
        }
    }
}