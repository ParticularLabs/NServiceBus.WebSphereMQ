namespace NServiceBus.Transports.WebSphereMQ.Receivers
{
    using System;
    using IBM.XMS;
    using Unicast.Transport;

    public interface IMessageReceiver
    {
        void Init(Address address, TransactionSettings settings, Func<TransportMessage, bool> tryProcessMessage,
                  Action<TransportMessage, Exception> endProcessMessage, Func<ISession, IMessageConsumer> createConsumer);

        void Start(int maximumConcurrencyLevel);
        void Stop();
    }
}