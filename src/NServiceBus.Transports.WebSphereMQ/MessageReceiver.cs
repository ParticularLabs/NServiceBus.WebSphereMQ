namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Schedulers;
    using System.Transactions;
    using CircuitBreakers;
    using IBM.XMS;
    using Logging;
    using Serializers.Json;
    using Utils;
    using TransactionSettings = Unicast.Transport.TransactionSettings;

    public class MessageReceiver
    {
        private const int MaximumDelay = 1000;
        private static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);
        private static readonly ILog Logger = LogManager.GetLogger(typeof (WebSphereMqDequeueStrategy));
        private readonly CircuitBreaker circuitBreaker = new CircuitBreaker(100, TimeSpan.FromSeconds(30));
        private readonly WebSphereMqSettings webSphereMqSettings;
        private readonly CancellationTokenSource tokenSource = new CancellationTokenSource();
        private Func<ISession, IMessageConsumer> createConsumer;
        private Action<string, Exception> endProcessMessage;
        private Address endpointAddress;
        private DateTime minimumJmsDate = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private MTATaskScheduler scheduler;
        private TransactionSettings transactionSettings;
        private TransactionOptions transactionOptions;
        private Func<TransportMessage, bool> tryProcessMessage;

        public MessageReceiver(WebSphereMqSettings settings)
        {
            webSphereMqSettings = settings;
        }

        public void Init(Address address, TransactionSettings settings, Func<TransportMessage, bool> tryProcessMessage,
                         Action<string, Exception> endProcessMessage, Func<ISession, IMessageConsumer> createConsumer)
        {
            endpointAddress = address;
            transactionSettings = settings;
            this.tryProcessMessage = tryProcessMessage;
            this.endProcessMessage = endProcessMessage;
            this.createConsumer = createConsumer;

            transactionOptions = new TransactionOptions
                {
                    IsolationLevel = settings.IsolationLevel,
                    Timeout = settings.TransactionTimeout
                };
        }

        public void Start(int maximumConcurrencyLevel)
        {
            Logger.InfoFormat("Starting MessageReceiver for [{0}].", endpointAddress);

            scheduler = new MTATaskScheduler(maximumConcurrencyLevel,
                                             String.Format("MessageReceiver Worker Thread for [{0}]",
                                                           endpointAddress));

            for (int i = 0; i < maximumConcurrencyLevel; i++)
            {
                StartConsumer();
            }

            Logger.InfoFormat(" MessageReceiver for [{0}] started with {1} worker threads.", endpointAddress,
                              maximumConcurrencyLevel);
        }

        public void Stop()
        {
            Logger.InfoFormat("Stopping MessageReceiver for [{0}].", endpointAddress);

            tokenSource.Cancel();
            scheduler.Dispose();

            Logger.InfoFormat("MessageReceiver for [{0}] stopped.", endpointAddress);
        }

        private void StartConsumer()
        {
            CancellationToken token = tokenSource.Token;

            Task.Factory
                .StartNew(Action, token, token, TaskCreationOptions.None, scheduler)
                .ContinueWith(t =>
                    {
                        t.Exception.Handle(ex =>
                            {
                                circuitBreaker.Execute(
                                    () =>
                                    Configure.Instance.RaiseCriticalError(
                                        string.Format("One of the MessageReceiver consumer threads for [{0}] crashed.",
                                                      endpointAddress), ex));
                                return true;
                            });

                        StartConsumer();
                    }, TaskContinuationOptions.OnlyOnFaulted);
        }

        private IConnection CreateConnection()
        {
            // Create the connection factories factory
            var factoryFactory = XMSFactoryFactory.GetInstance(XMSC.CT_WMQ);

            // Use the connection factories factory to create a connection factory
            var cf = factoryFactory.CreateConnectionFactory();

            // Set the properties
            cf.SetStringProperty(XMSC.WMQ_HOST_NAME, webSphereMqSettings.Hostname);
            cf.SetIntProperty(XMSC.WMQ_PORT, webSphereMqSettings.Port);
            cf.SetStringProperty(XMSC.WMQ_CHANNEL, webSphereMqSettings.Channel);
            cf.SetIntProperty(XMSC.WMQ_CONNECTION_MODE, XMSC.WMQ_CM_CLIENT);
            cf.SetStringProperty(XMSC.WMQ_QUEUE_MANAGER, webSphereMqSettings.QueueManager);
            //cf.SetIntProperty(XMSC.WMQ_CLIENT_RECONNECT_OPTIONS, XMSC.WMQ_CLIENT_RECONNECT);

            var clientId = String.Format("NServiceBus-{0}-{1}", Address.Local, Configure.DefineEndpointVersionRetriever());
            cf.SetStringProperty(XMSC.CLIENT_ID, Guid.NewGuid().ToString());

            var connection = cf.CreateConnection();

            connection.Start();

            return connection;
        }

        private void Action(object obj)
        {
            var cancellationToken = (CancellationToken) obj;
            var backOff = new BackOff(MaximumDelay);

            using (var connection = CreateConnection())
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = new ReceiveResult();

                    try
                    {
                        if (transactionSettings.IsTransactional)
                        {
                            if (!transactionSettings.DontUseDistributedTransactions)
                            {
                                RunInDistributedTransaction(connection, result);
                                continue;
                            }

                            RunInLocalTransaction(connection, result);
                            continue;
                        }

                        RunInNoTransaction(connection, result);
                    }
                    catch (Exception ex)
                    {
                        result.Exception = ex;
                    }
                    finally
                    {
                        endProcessMessage(result.MessageId, result.Exception);
                    }

                    backOff.Wait(() => result.MessageId == null);
                }
            }
        }

        private void RunInNoTransaction(IConnection connection, ReceiveResult result)
        {
            using (ISession session = connection.CreateSession(false, AcknowledgeMode.AutoAcknowledge))
            {
                using (IMessageConsumer consumer = createConsumer(session))
                {
                    IMessage message = consumer.Receive(delay);
                    if (message == null)
                    {
                        return;
                    }

                    result.MessageId = message.JMSMessageID;

                    ProcessMessage(message);
                }
            }
        }

        private int delay = 1000;

        private void RunInLocalTransaction(IConnection connection, ReceiveResult result)
        {
            using (ISession session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
            {
                WebSphereMqMessageSender.SetSession(session);

                using (IMessageConsumer consumer = createConsumer(session))
                {
                    IMessage message = consumer.Receive(delay);

                    if (message == null)
                    {
                        return;
                    }

                    result.MessageId = message.JMSMessageID;

                    if (ProcessMessage(message))
                    {
                        session.Commit();
                    }
                }
            }
        }

        private void RunInDistributedTransaction(IConnection connection, ReceiveResult result)
        {
            using (ISession session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
            {
                WebSphereMqMessageSender.SetSession(session);

                using (IMessageConsumer consumer = createConsumer(session))
                using (var scope = new TransactionScope(TransactionScopeOption.Required, transactionOptions))
                {
                    IMessage message = consumer.Receive(delay);

                    if (message == null)
                    {
                        return;
                    }

                    result.MessageId = message.JMSMessageID;

                    if (ProcessMessage(message))
                    {
                        scope.Complete();
                    }
                }
            }
        }

        private bool ProcessMessage(IMessage message)
        {
            TransportMessage transportMessage;
            try
            {
                transportMessage = ConvertToTransportMessage(message);
            }
            catch (Exception ex)
            {
                Logger.Error("Error in converting WebSphereMQ message to TransportMessage.", ex);

                //todo DeadLetter the message
                return true;
            }

            return tryProcessMessage(transportMessage);
        }

        private TransportMessage ConvertToTransportMessage(IMessage message)
        {
            var result = new TransportMessage
                {
                    Id = message.JMSMessageID,
                    CorrelationId = message.JMSCorrelationID,
                    Recoverable = message.JMSDeliveryMode == DeliveryMode.Persistent,
                };

            Address replyToAddress = message.JMSReplyTo == null
                                         ? null
                                         : Address.Parse(message.JMSReplyTo.Name);

            result.ReplyToAddress = replyToAddress;

            var textMessage = message as ITextMessage;
            if (textMessage != null && textMessage.Text != null)
            {
                result.Body = Encoding.UTF8.GetBytes(textMessage.Text);
            }

            var bytesMessage = message as IBytesMessage;
            if (bytesMessage != null && bytesMessage.BodyLength > 0)
            {
                var content = new byte[bytesMessage.BodyLength];
                bytesMessage.ReadBytes(content);
                result.Body = content;
            }

            if (message.JMSExpiration > 0)
            {
                result.TimeToBeReceived = minimumJmsDate.AddMilliseconds(message.JMSExpiration) - DateTime.UtcNow;
            }

            if (message.PropertyExists(Constants.NSB_HEADERS))
            {
                result.Headers =
                    Serializer.DeserializeObject<Dictionary<string, string>>(
                        message.GetStringProperty(Constants.NSB_HEADERS));
            }

            return result;
        }

        private class ReceiveResult
        {
            public Exception Exception { get; set; }
            public string MessageId { get; set; }
        }
    }
}