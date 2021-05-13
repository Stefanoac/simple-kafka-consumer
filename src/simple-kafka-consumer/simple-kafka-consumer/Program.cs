using simple_kafka_consumer.Services;

namespace simple_kafka_consumer
{
    class Program
    {
        public static void Main(string[] args)
        {
            var consumer = new KafkaConsumer();
            consumer.ReciveMessage();
        }
    }
}
