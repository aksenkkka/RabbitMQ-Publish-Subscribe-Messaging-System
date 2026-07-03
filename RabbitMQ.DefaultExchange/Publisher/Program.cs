using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading;
using System.Collections.Generic;

namespace Publisher
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                string exchangeName = "continents-topic-exchange";
                channel.ExchangeDeclare(exchange: exchangeName,
                                        type: ExchangeType.Topic,
                                        durable: false,
                                        autoDelete: false,
                                        arguments: null);

                string europeQueue       = "europe_queue";
                string asiaQueue         = "asia_queue";
                string northAmericaQueue = "north_america_queue";

                channel.QueueDeclare(queue: europeQueue,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                channel.QueueDeclare(queue: asiaQueue,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                channel.QueueDeclare(queue: northAmericaQueue,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                channel.QueueBind(queue: europeQueue,
                                  exchange: exchangeName,
                                  routingKey: "continent.europe");

                channel.QueueBind(queue: asiaQueue,
                                  exchange: exchangeName,
                                  routingKey: "continent.asia");

                channel.QueueBind(queue: northAmericaQueue,
                                  exchange: exchangeName,
                                  routingKey: "continent.north_america");

                string[] continents = {
                    "europe",
                    "asia",
                    "north_america"
                };

                Dictionary<string, int> counters = new Dictionary<string, int>();
                foreach (var c in continents)
                    counters[c] = 1;

                var random = new Random();

                while (true)
                {
                    Thread.Sleep(2000);

                    string continent = continents[random.Next(continents.Length)];
                    string routingKey = $"continent.{continent}";

                    string upperName = continent.Replace("_", " ").ToUpper();
                    string message = $"Message for {upperName}";

                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: exchangeName,
                                         routingKey: routingKey,
                                         basicProperties: null,
                                         body: body);

                    Console.WriteLine(
                        $"Message type {routingKey} is sent to {upperName} [N:{counters[continent]}]"
                    );

                    counters[continent]++;
                }
            }
        }
    }
}
