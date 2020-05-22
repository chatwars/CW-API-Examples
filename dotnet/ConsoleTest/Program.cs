using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace ConsoleTest {
    internal class Program {
        private static async Task Main(string[] args) {
            var consumer = new KafkaCwConsumer("digest-api.chtwrs.com:9092");

            await foreach (var entry in consumer.Consume()) {
                Console.WriteLine($"{entry.Topic} :: {entry.Message.Value}");
            }
        }
    }


    public class KafkaCwConsumer {
        private readonly ConsumerConfig config;

        public KafkaCwConsumer(string server) {
            config = new ConsumerConfig {
                                            BootstrapServers = server
                                          , Acks = Acks.All
                                          , AutoOffsetReset = AutoOffsetReset.Earliest
                                          , GroupId = "42131685-3c84-4602-9b6a-4f699c6f3f11"
                                        };
        }


        public async IAsyncEnumerable<ConsumeResult<Ignore, string>> Consume() {
            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(new[] {
                                         "cw3-sex_digest"
                                       , "cw3-deals"
                                       , "cw3-duels"
                                       , "cw3-offers"
                                       , "cw3-yellow_pages"
                                       , "cw3-au_digest"
                                     });


            while (true) {
                var cr = consumer.Consume();
                yield return cr;
            }
        }
    }
}