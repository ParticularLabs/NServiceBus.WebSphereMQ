﻿namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Collections;
    using IBM.WMQ;
    using IBM.WMQ.PCF;
    using MQC = IBM.XMS.MQC;

    public class QueueCreator : ICreateQueues
    {
        /// <summary>
        /// Settings
        /// </summary>
        public WebSphereMqSettings Settings { get; set; }

        public void CreateQueueIfNecessary(Address address, string account)
        {
            var queueName = WebSphereMqAddress.GetQueueName(address);

            var properties = new Hashtable
                {
                    {MQC.HOST_NAME_PROPERTY, Settings.Hostname},
                    {MQC.PORT_PROPERTY, Settings.Port},
                    {MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES},
                    {MQC.CHANNEL_PROPERTY, Settings.Channel},
                    {MQC.SSL_CIPHER_SPEC_PROPERTY, Settings.SslCipherSpec},
                    {MQC.SSL_CERT_STORE_PROPERTY, Settings.SslKeyRepository},
                    {MQC.SSL_PEER_NAME_PROPERTY, Settings.SslPeerName}
                };

            using (var queueManager = new MQQueueManager(Settings.QueueManager, properties))
            {
                var agent = new PCFMessageAgent(queueManager);
                var request = new PCFMessage(CMQCFC.MQCMD_CREATE_Q);
                request.AddParameter(MQC.MQCA_Q_NAME, queueName);
                request.AddParameter(MQC.MQIA_Q_TYPE, MQC.MQQT_LOCAL);
                request.AddParameter(MQC.MQIA_MAX_Q_DEPTH, Settings.MaxQueueDepth);

                try
                {
                    agent.Send(request);
                }
                catch (PCFException ex)
                {
                    if (ex.ReasonCode == PCFException.MQRCCF_CFST_STRING_LENGTH_ERR)
                    {
                        throw new ArgumentException(String.Format("Queue name too long:{0}", queueName));
                    }

                    if (ex.ReasonCode != PCFException.MQRCCF_OBJECT_ALREADY_EXISTS)
                    {
                        throw;
                    }
                }
            }
        }
    }
}