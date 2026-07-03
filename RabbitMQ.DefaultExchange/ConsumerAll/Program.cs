using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsumerAll
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            string exchangeName = "continents-topic-exchange";
            string allQueue = "all_continents_queue";

            channel.ExchangeDeclare(exchange: exchangeName,
                                    type: ExchangeType.Topic,
                                    durable: false,
                                    autoDelete: false,
                                    arguments: null);

            channel.QueueDeclare(queue: allQueue,
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            channel.QueueBind(queue: allQueue,
                              exchange: exchangeName,
                              routingKey: "continent.#");

            string[] continents = { "europe", "asia", "north_america" };

            var counters = new Dictionary<string, int>();
            foreach (var c in continents)
                counters[c] = 1;

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var routingKey = ea.RoutingKey;
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                string[] parts = routingKey.Split('.');
                string continent = parts.Length > 1 ? parts[1] : "unknown";

                if (!counters.ContainsKey(continent))
                    counters[continent] = 1;

                int current = counters[continent];

                Console.WriteLine(
                    $"Consumer received: Message type ({routingKey}) from publisher [N:{current}]"
                );

                counters[continent]++;
            };

            channel.BasicConsume(queue: allQueue,
                                 autoAck: true,
                                 consumer: consumer);

            Console.ReadLine();
        }
    }
}
