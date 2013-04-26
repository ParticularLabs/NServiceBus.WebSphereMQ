namespace NServiceBus.Transports.WebSphereMQ.Config
{
    using Unicast.Queuing.Installers;
    using Unicast.Subscriptions;
    using WebSphereMQ = NServiceBus.WebSphereMQ;

    public class WebSphereMqTransportConfigurer : ConfigureTransport<WebSphereMQ>
    {
        protected override string ExampleConnectionStringForErrorMessage
        {
            get { return "hostname=localhost;queueManager=QM_TEST;"; }
        }

        protected override void InternalConfigure(Configure config, string connectionString)
        {
            Address.IgnoreMachineName();

            var parser = new WebSphereMqConnectionStringBuilder(connectionString);
            var settings = parser.RetrieveSettings();

            config.Configurer.RegisterSingleton<WebSphereMqSettings>(settings);

            config.Configurer.ConfigureComponent<WebSphereMqConnectionFactory>(DependencyLifecycle.SingleInstance);

            config.Configurer.ConfigureComponent<WebSphereMqMessageSender>(DependencyLifecycle.InstancePerCall);

            config.Configurer.ConfigureComponent<WebSphereMqSubscriptionsManager>(DependencyLifecycle.SingleInstance);

            config.Configurer.ConfigureComponent<MessageReceiver>( DependencyLifecycle.InstancePerCall);

            config.Configurer.ConfigureComponent<WebSphereMqMessagePublisher>(DependencyLifecycle.InstancePerCall);

            config.Configurer.ConfigureComponent<WebSphereMqQueueCreator>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.Settings, settings);
            
            config.Configurer.ConfigureComponent<WebSphereMqDequeueStrategy>(DependencyLifecycle.InstancePerCall)
                .ConfigureProperty(p => p.PurgeOnStartup, ConfigurePurging.PurgeRequested)
                .ConfigureProperty(p => p.Settings, settings);
            
            config.Configurer.ConfigureComponent<NoConfigRequiredAutoSubscriptionStrategy>(DependencyLifecycle.InstancePerCall);

            EndpointInputQueueCreator.Enabled = true;
        }

      

    }
}