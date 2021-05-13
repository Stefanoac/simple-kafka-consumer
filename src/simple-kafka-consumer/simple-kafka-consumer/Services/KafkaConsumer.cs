using Confluent.Kafka;
using Serilog;
using System;
using System.Threading;

namespace simple_kafka_consumer.Services
{
    public class KafkaConsumer
    {
        public const String HOST = "127.0.0.1:9092";
        public const String KAFKA_TOPIC_NAME = "test-topic";
        public const String CONSUMER_GROUP_ID = "test-consumer-group";

        public void ReciveMessage()
        {
            Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();

            var conf = new ConsumerConfig
            {
                GroupId = CONSUMER_GROUP_ID,
                BootstrapServers = HOST,
                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe(KAFKA_TOPIC_NAME);

                var cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}
