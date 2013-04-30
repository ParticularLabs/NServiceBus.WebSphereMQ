namespace NServiceBus.Transports.WebSphereMQ
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
            var properties = new Hashtable
                {
                    {MQC.HOST_NAME_PROPERTY, Settings.Hostname},
                    {MQC.PORT_PROPERTY, Settings.Port},
                    {MQC.TRANSPORT_PROPERTY, MQC.TRANSPORT_MQSERIES_MANAGED},
                    {MQC.CHANNEL_PROPERTY, Settings.Channel}
                };

            using (var queueManager = new MQQueueManager(Settings.QueueManager, properties))
            {
                var agent = new PCFMessageAgent(queueManager);
                var request = new PCFMessage(CMQCFC.MQCMD_CREATE_Q);
                request.AddParameter(MQC.MQCA_Q_NAME, address.Queue);
                request.AddParameter(MQC.MQIA_Q_TYPE, MQC.MQQT_LOCAL);

                try
                {
                    agent.Send(request);
                }
                catch (PCFException ex)
                {
                    if (ex.ReasonCode == PCFException.MQRCCF_CFST_STRING_LENGTH_ERR)
                    {
                        throw new ArgumentException(String.Format("Queue name too long:{0}", address.Queue));
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