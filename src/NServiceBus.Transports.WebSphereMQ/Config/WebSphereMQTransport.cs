namespace NServiceBus.Transports.WebSphereMQ.Config
{
    using Features;
    using NServiceBus.Config;
    using Settings;
    using Unicast.Queuing.Installers;
    using Unicast.Subscriptions;
    using WebSphereMQ = NServiceBus.WebSphereMQ;

    public class WebSphereMQTransport : ConfigureTransport<WebSphereMQ>, IFeature
    {
        protected override string ExampleConnectionStringForErrorMessage
        {
            get { return "hostname=localhost;queueManager=QM_TEST;"; }
        }

        public void Initialize()
        {
            Address.IgnoreMachineName();

            var connectionString = SettingsHolder.Get<string>("NServiceBus.Transport.ConnectionString");
            var parser = new ConnectionStringBuilder(connectionString);
            var settings = parser.RetrieveSettings();

            NServiceBus.Configure.Instance.Configurer.RegisterSingleton<WebSphereMqSettings>(settings);

            NServiceBus.Configure.Component<ConnectionFactory>(DependencyLifecycle.SingleInstance);
            NServiceBus.Configure.Component<SessionFactory>(DependencyLifecycle.SingleInstance);
            NServiceBus.Configure.Component<SubscriptionsManager>(DependencyLifecycle.SingleInstance);

            NServiceBus.Configure.Component<MessageSender>(DependencyLifecycle.InstancePerCall);
            NServiceBus.Configure.Component<MessageReceiver>(DependencyLifecycle.InstancePerCall);
            NServiceBus.Configure.Component<MessagePublisher>(DependencyLifecycle.InstancePerCall);
            NServiceBus.Configure.Component<QueueCreator>(DependencyLifecycle.InstancePerCall)
                       .ConfigureProperty(p => p.Settings, settings);
            NServiceBus.Configure.Component<DequeueStrategy>(DependencyLifecycle.InstancePerCall)
                       .ConfigureProperty(p => p.PurgeOnStartup, ConfigurePurging.PurgeRequested)
                       .ConfigureProperty(p => p.Settings, settings);

            EndpointInputQueueCreator.Enabled = true;
        }

        protected override void InternalConfigure(Configure config, string connectionString)
        {
            Feature.Enable<WebSphereMQTransport>();

            InfrastructureServices.RegisterServiceFor<IAutoSubscriptionStrategy>(
                typeof (NoConfigRequiredAutoSubscriptionStrategy), DependencyLifecycle.InstancePerCall);
        }
    }
}