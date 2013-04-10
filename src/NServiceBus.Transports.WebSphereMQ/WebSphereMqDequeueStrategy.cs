namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Threading.Tasks.Schedulers;
    using System.Transactions;
    using CircuitBreakers;
    using IBM.WMQ;
    using IBM.WMQ.PCF;
    using IBM.XMS;
    using Logging;
    using Serializers.Json;
    using Unicast.Transport;
    using MQC = IBM.XMS.MQC;

    public class WebSphereMqDequeueStrategy : IDequeueMessages
    {
        public WebSphereMqDequeueStrategy(WebSphereMqConnectionFactory factory)
        {
            this.factory = factory;
        }

        /// <summary>
        ///     Purges the queue on startup.
        /// </summary>
        public bool PurgeOnStartup { get; set; }

        /// <summary>
        /// Settings
        /// </summary>
        public WebSphereMqSettings Settings { get; set; }

        /// <summary>
        /// Message sender
        /// </summary>
        public WebSphereMqMessageSender MessageSender { get; set; }

        public void Init(Address address, TransactionSettings settings, Func<TransportMessage, bool> tryProcessMessage, Action<string, Exception> endProcessMessage)
        {
            this.tryProcessMessage = tryProcessMessage;
            this.endProcessMessage = endProcessMessage;
            endpointAddress = address;
            transactionSettings = settings;
            transactionOptions = new TransactionOptions { IsolationLevel = transactionSettings.IsolationLevel, Timeout = transactionSettings.TransactionTimeout };
        }

        public void Start(int maximumConcurrencyLevel)
        {
            if (PurgeOnStartup)
            {
                Purge();
            }

            scheduler = new MTATaskScheduler(maximumConcurrencyLevel,
                                             String.Format("NServiceBus Dequeuer Worker Thread for [{0}]", endpointAddress));

            connection = factory.CreateConnection();

            for (int i = 0; i < maximumConcurrencyLevel; i++)
            {
                StartConsumer();
            }
        }

        public void Purge()
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
                request.AddParameter(MQC.MQCA_Q_NAME, endpointAddress.Queue);

                try
                {
                    agent.Send(request);
                }
                catch (PCFException ex)
                {
                    Logger.Warn(string.Format("Could not purge queue ({0}) at startup", endpointAddress), ex);
                }
            }
        }

        private void StartConsumer()
        {
            var token = tokenSource.Token;

            Task.Factory
                .StartNew(Action, token, token, TaskCreationOptions.None, scheduler)
                .ContinueWith(t =>
                {
                    t.Exception.Handle(ex =>
                    {
                        circuitBreaker.Execute(() => Configure.Instance.RaiseCriticalError("Failed to start consumer.", ex));
                        return true;
                    });

                    StartConsumer();
                }, TaskContinuationOptions.OnlyOnFaulted);
        }

        private void Action(object obj)
        {
            var cancellationToken = (CancellationToken) obj;

            while (!cancellationToken.IsCancellationRequested)
            {
                using (var session = connection.CreateSession(transactionSettings.IsTransactional,
                                                              transactionSettings.IsTransactional
                                                                  ? AcknowledgeMode.AutoAcknowledge
                                                                  : AcknowledgeMode.DupsOkAcknowledge))
                using (var destination = session.CreateQueue(endpointAddress.Queue))
                using (var consumer = session.CreateConsumer(destination))
                {
                    Exception exception = null;
                    IMessage message = null;

                    try
                    {
                        if (transactionSettings.IsTransactional)
                        {
                            if (MessageSender != null)
                            {
                                MessageSender.SetSession(session);
                            }

                            if (!transactionSettings.DontUseDistributedTransactions) //Using distributed transactions
                            {
                                using (var scope = new TransactionScope(TransactionScopeOption.Required, transactionOptions))
                                {
                                    message = consumer.Receive(1000);

                                    if (message == null)
                                    {
                                        scope.Complete();
                                        continue;
                                    }

                                    if (ProcessMessage(message))
                                    {
                                        scope.Complete();
                                    }
                                }
                            }
                            else // Using Local transactions
                            {
                                message = consumer.Receive(1000);

                                if (message == null)
                                {
                                    continue;
                                }

                                if (ProcessMessage(message))
                                {
                                    session.Commit();
                                }
                            }
                        }
                        else // No transaction at all
                        {
                            message = consumer.Receive(1000);

                            if (message == null)
                            {
                                continue;
                            }

                            ProcessMessage(message);
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
                Logger.Error("Error in converting message to TransportMessage.", ex);

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

            var replyToAddress = message.JMSReplyTo == null
                                     ? null
                                     : Address.Parse(message.JMSReplyTo.Name);

            result.ReplyToAddress = replyToAddress;

            var textMessage = message as ITextMessage;
            if (textMessage != null)
            {
                result.Body = Encoding.UTF8.GetBytes(textMessage.Text);
            }

            var bytesMessage = message as IBytesMessage;
            if (bytesMessage != null)
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
                result.Headers = Serializer.DeserializeObject<Dictionary<string, string>>(message.GetStringProperty(Constants.NSB_HEADERS));
            }

            return result;
        }

        public void Stop()
        {
            Logger.Debug(String.Format("NServiceBus Dequeuer Worker Thread for [{0}] stop called.", endpointAddress));
            tokenSource.Cancel();
            scheduler.Dispose();
        }

        IConnection connection;
        DateTime minimumJmsDate = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        readonly CircuitBreaker circuitBreaker = new CircuitBreaker(100, TimeSpan.FromSeconds(30));
        Func<TransportMessage, bool> tryProcessMessage;
        static readonly ILog Logger = LogManager.GetLogger(typeof(WebSphereMqDequeueStrategy));
        MTATaskScheduler scheduler;
        readonly CancellationTokenSource tokenSource = new CancellationTokenSource();
        TransactionSettings transactionSettings;
        Address endpointAddress;
        TransactionOptions transactionOptions;
        Action<string, Exception> endProcessMessage;
        readonly WebSphereMqConnectionFactory factory;
        static readonly JsonMessageSerializer Serializer = new JsonMessageSerializer(null);
    }
}