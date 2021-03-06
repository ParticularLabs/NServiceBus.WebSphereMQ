﻿namespace NServiceBus.Transports.WebSphereMQ.Senders
{
    using System.Transactions;
    using IBM.XMS;

    public class LocalTransactionMessageSender : MessageSender
    {
        public CurrentSessions CurrentSessions { get; set; }

        protected override void InternalSend()
        {
            var hasExistingSession = true;
            var session = CurrentSessions.GetSession();

            if (session == null)
            {
                hasExistingSession = false;
                session = CreateSession();
            }

            try
            {

                using (var destination = destinationAddress.CreateDestination(session))
                using (var producer = session.CreateProducer(destination))
                {
                    var mqMessage = CreateNativeMessage(session, producer);

                    using (new TransactionScope(TransactionScopeOption.Suppress))
                    {
                        producer.Send(mqMessage);
                    }

                    producer.Close();
                }
            }
            finally
            {
                if (!hasExistingSession)
                {
                    session.Close();
                    session.Dispose();
                }
            }
        }
    }
}