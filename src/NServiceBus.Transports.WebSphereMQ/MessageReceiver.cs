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
    using Unicast.Transport;
    using Utils;

    public class MessageReceiver
    {
        private const int MaximumDelay = 1000;
        private static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);
        private static readonly ILog Logger = LogManager.GetLogger(typeof (WebSphereMqDequeueStrategy));
        private readonly CircuitBreaker circuitBreaker = new CircuitBreaker(100, TimeSpan.FromSeconds(30));
        private readonly WebSphereMqConnectionFactory factory;
        private readonly WebSphereMqMessageSender messageSender;
        private readonly CancellationTokenSource tokenSource = new CancellationTokenSource();
        private IConnection connection;
        private Func<ISession, IMessageConsumer> createConsumer;
        private Action<string, Exception> endProcessMessage;
        private Address endpointAddress;
        private DateTime minimumJmsDate = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private MTATaskScheduler scheduler;
        private TransactionSettings settings;
        private TransactionOptions transactionOptions;
        private Func<TransportMessage, bool> tryProcessMessage;

        public MessageReceiver(WebSphereMqConnectionFactory factory, WebSphereMqMessageSender messageSender)
        {
            this.factory = factory;
            this.messageSender = messageSender;
        }

        public void Init(Address address, TransactionSettings settings, Func<TransportMessage, bool> tryProcessMessage,
                         Action<string, Exception> endProcessMessage, Func<ISession, IMessageConsumer> createConsumer)
        {
            endpointAddress = address;
            this.settings = settings;
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

            connection = factory.CreateConnection();

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

        private void Action(object obj)
        {
            var cancellationToken = (CancellationToken) obj;
            var backOff = new BackOff(MaximumDelay);

            while (!cancellationToken.IsCancellationRequested)
            {
                var result = new ReceiveResult();

                try
                {
                    if (settings.IsTransactional)
                    {
                        if (!settings.DontUseDistributedTransactions)
                        {
                            RunInDistributedTransaction(result);
                            continue;
                        }

                        RunInLocalTransaction(result);
                        continue;
                    }

                    RunInNoTransaction(result);
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

        private void RunInNoTransaction(ReceiveResult result)
        {
            using (ISession session = connection.CreateSession(false, AcknowledgeMode.DupsOkAcknowledge))
            {
                using (IMessageConsumer consumer = createConsumer(session))
                {
                    IMessage message = consumer.ReceiveNoWait();
                    if (message == null)
                    {
                        return;
                    }

                    result.MessageId = message.JMSMessageID;

                    ProcessMessage(message);
                }
            }
        }

        private void RunInLocalTransaction(ReceiveResult result)
        {
            using (ISession session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
            {
                messageSender.SetSession(session);

                using (IMessageConsumer consumer = createConsumer(session))
                {
                    IMessage message = consumer.ReceiveNoWait();

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

        private void RunInDistributedTransaction(ReceiveResult result)
        {
            using (var scope = new TransactionScope(TransactionScopeOption.Required, transactionOptions))
            using (ISession session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
            {
                messageSender.SetSession(session);

                using (IMessageConsumer consumer = createConsumer(session))
                {
                    IMessage message = consumer.ReceiveNoWait();

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