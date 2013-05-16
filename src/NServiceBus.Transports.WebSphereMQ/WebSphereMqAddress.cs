namespace NServiceBus.Transports.WebSphereMQ
{
    using System;
    using System.Collections.Generic;
    using System.Security.Cryptography;
    using System.Text;
    using IBM.XMS;
    using Logging;

    public class WebSphereMqAddress
    {
        readonly Address address;

        public WebSphereMqAddress(Address address)
        {
            this.address = address;
        }

        public static string GetQueueName(Address address)
        {
            return GenerateQueueName(address);
        }

        public string QueueName
        {
            get
            {
                return  GenerateQueueName(address);;
            }
        }


        public bool IsTopic
        {
            get
            {
                return QueueName.StartsWith("topic://");
            }
        }

        public IDestination CreateDestination(ISession session)
        {
            return IsTopic ? session.CreateTopic(QueueName) : session.CreateQueue(QueueName);
        }


        static string GenerateQueueName(Address address)
        {
            var queue = address.Queue;

            if (queueNames.ContainsKey(queue))
                return queueNames[queue];

            if (queue.Length <= 48)
                return queue;

            var queueHash = DeterministicGuidBuilder(queue).ToString().Replace("-","");

            var shortenedQueueName = queue.Substring(0, 48 - queueHash.Length - 1) + "." + queueHash;
            
            Logger.WarnFormat("Queue name was longer than the 48 char limit imposed by WMQ. Name changed from: {0} to {1}",queue,shortenedQueueName);

            queueNames[queue] = shortenedQueueName;

            return shortenedQueueName;
        }

       

        static Guid DeterministicGuidBuilder(string input)
        {
            //use MD5 hash to get a 16-byte hash of the string
            using (var provider = new MD5CryptoServiceProvider())
            {
                byte[] inputBytes = Encoding.Default.GetBytes(input);
                byte[] hashBytes = provider.ComputeHash(inputBytes);
                //generate a guid from the hash:
                return new Guid(hashBytes);
            }
        }

        static readonly IDictionary<string,string> queueNames = new Dictionary<string,string>();
        static readonly ILog Logger = LogManager.GetLogger(typeof(WebSphereMqAddress));
      
    }
}