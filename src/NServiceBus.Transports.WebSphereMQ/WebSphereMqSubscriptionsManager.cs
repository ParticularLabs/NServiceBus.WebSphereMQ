namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using IBM.XMS;
    using Logging;
    using ObjectBuilder;
    using Unicast.Transport;

    public class WebSphereMqSubscriptionsManager : IManageSubscriptions
    {
        private readonly WebSphereMqConnectionFactory factory;

        private readonly BlockingCollection<Tuple<Type, Address>> events =
            new BlockingCollection<Tuple<Type, Address>>();

        private readonly List<EventConsumerSatellite> satellites = new List<EventConsumerSatellite>();
        private Action<string, Exception> endProcessMessage;

        private Thread startSubscriptionThread;
        private TransactionSettings settings;
        private Func<TransportMessage, bool> tryProcessMessage;
        static readonly ILog Logger = LogManager.GetLogger(typeof(WebSphereMqSubscriptionsManager));

        public IBuilder Builder { get; set; }

        public WebSphereMqSubscriptionsManager(WebSphereMqConnectionFactory factory)
        {
            this.factory = factory;
        }

        public void Subscribe(Type eventType, Address publisherAddress)
        {
            events.TryAdd(Tuple.Create(eventType, publisherAddress));
        }

        public void Unsubscribe(Type eventType, Address publisherAddress)
        {
            EventConsumerSatellite consumerSatellite =
                satellites.Find(
                    consumer =>
                    consumer.InputAddress == publisherAddress && consumer.EventType == eventType.FullName);

            if (consumerSatellite == null)
            {
                return;
            }

            consumerSatellite.Stop();
            satellites.Remove(consumerSatellite);

            var connection = factory.CreateConnection();
            using (ISession session = connection.CreateSession(false, AcknowledgeMode.DupsOkAcknowledge))
            {
                session.Unsubscribe(eventType.FullName);
            }
        }

        public void Init(TransactionSettings settings, Func<TransportMessage, bool> tryProcessMessage,
                         Action<string, Exception> endProcessMessage)
        {
            this.settings = settings;
            this.tryProcessMessage = tryProcessMessage;
            this.endProcessMessage = endProcessMessage;
        }

        public void Start(int maximumConcurrencyLevel)
        {
            startSubscriptionThread = new Thread(() =>
                {
                    foreach (var tuple in events.GetConsumingEnumerable())
                    {
                        var messageReceiver = Builder.Build<MessageReceiver>();
                        Address address =
                            Address.Parse(String.Format("topic://{0}/EVENTS/#/{1}/#", tuple.Item2.Queue,
                                                        tuple.Item1.FullName));
                        var consumerSatellite = new EventConsumerSatellite(messageReceiver, address,
                                                                           tuple.Item1.FullName);

                        messageReceiver.Init(address, settings, tryProcessMessage, endProcessMessage, consumerSatellite.CreateConsumer);

                        satellites.Add(consumerSatellite);
                        Logger.InfoFormat("Starting receiver for [{0}] subscription.", address);

                        consumerSatellite.Start(maximumConcurrencyLevel);
                    }
                }) {IsBackground = true};

            startSubscriptionThread.SetApartmentState(ApartmentState.MTA);
            startSubscriptionThread.Name = "Start WebSphereMq Subscription Listeners";
            startSubscriptionThread.Start();
        }

        public void Stop()
        {
            events.CompleteAdding();

            foreach (EventConsumerSatellite consumerSatellite in satellites)
            {
                consumerSatellite.Stop();
            }
        }

        private class EventConsumerSatellite
        {
            private readonly Address address;
            private readonly string eventType;
            private readonly MessageReceiver receiver;

            public EventConsumerSatellite(MessageReceiver receiver, Address address, string eventType)
            {
                this.receiver = receiver;
                this.eventType = eventType;

                this.address = address;
            }

            public IMessageConsumer CreateConsumer(ISession session)
            {
                return session.CreateDurableSubscriber(session.CreateTopic(address.Queue), eventType);
            }

            public Address InputAddress
            {
                get { return address; }
            }

            public string EventType
            {
                get { return eventType; }
            }

            public void Start(int maximumConcurrencyLevel)
            {
                receiver.Start(maximumConcurrencyLevel);
            }

            public void Stop()
            {
                receiver.Stop();
            }
        }
    }
}