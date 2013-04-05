namespace NServiceBus.Transports.WebSphereMQ
{
    using System;

    public static class TopicNameCreator
    {
        public static string GetName(Type eventType)
        {
            return eventType.FullName;
        }
    }
}