using System.Runtime.Serialization;

namespace com.github.dhoard.kafka
{
    [Serializable]
    internal class IllegalArgumentException : Exception
    {
        public IllegalArgumentException()
        {
        }

        public IllegalArgumentException(string? message) : base(message)
        {
        }

        public IllegalArgumentException(string? message, Exception? innerException) : base(message, innerException)
        {
        }

        protected IllegalArgumentException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}