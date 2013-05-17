namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Collections;
    using IBM.WMQ;
    using IBM.WMQ.PCF;
    using Logging;
    using Receivers;
    using Unicast.Transport;
    using MQC = IBM.XMS.MQC;

    public class DequeueStrategy : IDequeueMessages
    {
        private readonly SubscriptionsManager subscriptionsManager;
        private readonly IMessageReceiver messageReceiver;

        /// <summary>
        ///     Purges the queue on startup.
        /// </summary>
        public bool PurgeOnStartup { get; set; }

        /// <summary>
        /// Settings
        /// </summary>
        public WebSphereMqSettings Settings { get; set; }

        public DequeueStrategy(SubscriptionsManager subscriptionsManager, IMessageReceiver messageReceiver)
        {
            this.subscriptionsManager = subscriptionsManager;
            this.messageReceiver = messageReceiver;
        }

        public void Init(Address address, TransactionSettings settings, Func<TransportMessage, bool> tryProcessMessage,
                         Action<TransportMessage, Exception> endProcessMessage)
        {
            isTheMainTransport = address == Address.Local;

            endpointAddress = new WebSphereMqAddress(address);

            if (address == Address.Local)
            {
                subscriptionsManager.Init(settings, tryProcessMessage, endProcessMessage);
            }

            messageReceiver.Init(address, settings, tryProcessMessage, endProcessMessage, session => session.CreateConsumer(session.CreateQueue(endpointAddress.QueueName)));
        }

        public void Start(int maximumConcurrencyLevel)
        {
            if (PurgeOnStartup)
            {
                Purge();
            }

            messageReceiver.Start(maximumConcurrencyLevel);

            if (isTheMainTransport)
            {
                subscriptionsManager.Start(1);
            }
        }

        public void Stop()
        {
            if (isTheMainTransport)
            {
                subscriptionsManager.Stop();
            }

            messageReceiver.Stop();
        }

        private void Purge()
        {
            var properties = new Hashtable
                {
                    {MQC.HOST_NAME_PROPERTY, Settings.Hostname},
                    {MQC.PORT_PROPERTY, Settings.Port},
                    {MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_CLIENT},
                    {MQC.CHANNEL_PROPERTY, Settings.Channel}
                };

            using (var queueManager = new MQQueueManager(Settings.QueueManager, properties))
            {
                var agent = new PCFMessageAgent(queueManager);
                var request = new PCFMessage(CMQCFC.MQCMD_CLEAR_Q);
                request.AddParameter(MQC.MQCA_Q_NAME, endpointAddress.QueueName);

                try
                {
                    agent.Send(request);
                }
                catch (PCFException ex)
                {
                    Logger.Warn(string.Format("Could not purge queue ({0}) at startup", endpointAddress.QueueName), ex);
                }
            }
        }


        static readonly ILog Logger = LogManager.GetLogger(typeof(DequeueStrategy));
        WebSphereMqAddress endpointAddress;
        bool isTheMainTransport;
    }
}