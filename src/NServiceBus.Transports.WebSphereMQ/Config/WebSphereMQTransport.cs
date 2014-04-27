﻿namespace NServiceBus.Transports.WebSphereMQ.Config
{
    using Receivers;
    using Senders;
    using Settings;
    using WebSphereMQ = NServiceBus.WebSphereMQ;
    using System;

    public class WebSphereMQTransport : ConfigureTransport<WebSphereMQ>
    {
        protected override string ExampleConnectionStringForErrorMessage
        {
            get { return "hostname=localhost;queueManager=QM_TEST;"; }
        }

        public override void Initialize()
        {
            Address.IgnoreMachineName();

            Environment.SetEnvironmentVariable("AMQ_SSL_ALLOW_DEFAULT_CERT", "true");

            var connectionString = SettingsHolder.Get<string>("NServiceBus.Transport.ConnectionString");
            var parser = new ConnectionStringBuilder(connectionString);
            var settings = parser.RetrieveSettings();

            NServiceBus.Configure.Instance.Configurer.RegisterSingleton<WebSphereMqSettings>(settings);

            NServiceBus.Configure.Component<ConnectionFactory>(DependencyLifecycle.SingleInstance);
            NServiceBus.Configure.Component<CurrentSessions>(DependencyLifecycle.SingleInstance);
            NServiceBus.Configure.Component<SubscriptionsManager>(DependencyLifecycle.SingleInstance);

            var transactionSettings = new Unicast.Transport.TransactionSettings();

            if (transactionSettings.IsTransactional)
            {
                if (!transactionSettings.DontUseDistributedTransactions)
                {
                    NServiceBus.Configure.Component<DistributedTransactionMessageReceiver>(DependencyLifecycle.InstancePerCall);
                    NServiceBus.Configure.Component<DistributedTransactionMessageSender>(DependencyLifecycle.InstancePerCall);

                }
                else
                {
                    NServiceBus.Configure.Component<LocalTransactionMessageSender>(DependencyLifecycle.InstancePerCall);
                    NServiceBus.Configure.Component<LocalTransactionMessageReceiver>(DependencyLifecycle.InstancePerCall);
                }
            }
            else
            {
                NServiceBus.Configure.Component<NoTransactionMessageSender>(DependencyLifecycle.InstancePerCall);
                NServiceBus.Configure.Component<NoTransactionMessageReceiver>(DependencyLifecycle.InstancePerCall);
            }

            NServiceBus.Configure.Component<MessagePublisher>(DependencyLifecycle.InstancePerCall);
            NServiceBus.Configure.Component<QueueCreator>(DependencyLifecycle.InstancePerCall)
                       .ConfigureProperty(p => p.Settings, settings);
            NServiceBus.Configure.Component<DequeueStrategy>(DependencyLifecycle.InstancePerCall)
                       .ConfigureProperty(p => p.PurgeOnStartup, ConfigurePurging.PurgeRequested)
                       .ConfigureProperty(p => p.Settings, settings);
        }

        protected override void InternalConfigure(Configure config)
        {
            Enable<WebSphereMQTransport>();
        }
    }
}