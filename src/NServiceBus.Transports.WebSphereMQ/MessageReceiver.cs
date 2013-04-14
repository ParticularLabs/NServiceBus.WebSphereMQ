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

    public class MessageReceiver
    {
        private static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);
        private static readonly ILog Logger = LogManager.GetLogger(typeof (WebSphereMqDequeueStrategy));
        private readonly CircuitBreaker circuitBreaker = new CircuitBreaker(100, TimeSpan.FromSeconds(30));
        private readonly WebSphereMqConnectionFactory factory;
        private readonly WebSphereMqMessageSender messageSender;
        private readonly CancellationTokenSource tokenSource = new CancellationTokenSource();
        private IConnection connection;
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
                         Action<string, Exception> endProcessMessage)
        {
            endpointAddress = address;
            this.settings = settings;
            this.tryProcessMessage = tryProcessMessage;
            this.endProcessMessage = endProcessMessage;

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

            Logger.InfoFormat(" MessageReceiver for [{0}] started with {1} worker threads.", endpointAddress, maximumConcurrencyLevel);
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
                                    () => Configure.Instance.RaiseCriticalError(string.Format("One of the MessageReceiver consumer threads for [{0}] crashed.", endpointAddress), ex));
                                return true;
                            });

                        StartConsumer();
                    }, TaskContinuationOptions.OnlyOnFaulted);
        }

        static bool IsTopic(Address address)
        {
            return address.Queue.StartsWith("topic://");
        }

        const int delay = 1000;

        private void Action(object obj)
        {
            var cancellationToken = (CancellationToken) obj;

            while (!cancellationToken.IsCancellationRequested)
            {
                if (settings.IsTransactional)
                {
                    if (!settings.DontUseDistributedTransactions)
                    {
                        RunInDistributedTransaction();
                        continue;
                    }

                    RunInLocalTransaction();
                    continue;
                }

                RunInNoTransaction();
            }
        }

        private void RunInNoTransaction()
        {
            using (var session = connection.CreateSession(false, AcknowledgeMode.DupsOkAcknowledge))
            {
                IDestination destination;
                IMessageConsumer consumer;
                if (IsTopic(endpointAddress))
                {
                    destination = session.CreateTopic(endpointAddress.Queue);
                    consumer = session.CreateDurableSubscriber(destination,
                                                               endpointAddress.Queue.GetHashCode().ToString());
                }
                else
                {
                    destination = session.CreateQueue(endpointAddress.Queue);
                    consumer = session.CreateConsumer(destination);
                }
                using (destination)
                using (consumer)
                {
                    Exception exception = null;
                    IMessage message = null;

                    try
                    {
                        message = consumer.Receive(delay);
                        if (message == null)
                        {
                            return;
                        }

                        ProcessMessage(message);
                    }
                    catch (Exception ex)
                    {
                        exception = ex;
                    }
                    finally
                    {
                        endProcessMessage(message != null ? message.JMSMessageID : null, exception);
                    }
                }
            }
        }

        private void RunInLocalTransaction()
        {
            using (var session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
                {
                    IDestination destination;
                    IMessageConsumer consumer;
                    if (IsTopic(endpointAddress))
                    {
                        destination = session.CreateTopic(endpointAddress.Queue);
                        consumer = session.CreateDurableSubscriber(destination, endpointAddress.Queue.GetHashCode().ToString());
                    }
                    else
                    {
                        destination = session.CreateQueue(endpointAddress.Queue);
                        consumer = session.CreateConsumer(destination);
                    }
                    using(destination)
                    using (consumer)
                    {
                        Exception exception = null;
                        IMessage message = null;

                        try
                        {
                            message = consumer.Receive(delay);

                            if (message == null)
                            {
                                return;
                            }

                            if (ProcessMessage(message))
                            {
                                session.Commit();
                            }
                        }
                        catch (Exception ex)
                        {
                            exception = ex;
                        }
                        finally
                        {
                            endProcessMessage(message != null ? message.JMSMessageID : null, exception);
                        }
                    }
                }
        }

        private void RunInDistributedTransaction()
        {
            using (var scope = new TransactionScope(TransactionScopeOption.Required, transactionOptions))
            using (var session = connection.CreateSession(true, AcknowledgeMode.AutoAcknowledge))
            {
                messageSender.SetSession(session);

                IDestination destination;
                IMessageConsumer consumer;
                if (IsTopic(endpointAddress))
                {
                    destination = session.CreateTopic(endpointAddress.Queue);
                    consumer = session.CreateDurableSubscriber(destination,
                                                               endpointAddress.Queue.GetHashCode().ToString());
                }
                else
                {
                    destination = session.CreateQueue(endpointAddress.Queue);
                    consumer = session.CreateConsumer(destination);
                }
                using (destination)
                using (consumer)
                {
                    Exception exception = null;
                    IMessage message = null;

                    try
                    {
                        message = consumer.Receive(delay);

                        if (message == null)
                        {
                            scope.Complete();
                            return;
                        }

                        if (ProcessMessage(message))
                        {
                            scope.Complete();
                        }
                    }
                    catch (Exception ex)
                    {
                        exception = ex;
                    }
                    finally
                    {
                        endProcessMessage(message != null ? message.JMSMessageID : null, exception);
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
    }
}