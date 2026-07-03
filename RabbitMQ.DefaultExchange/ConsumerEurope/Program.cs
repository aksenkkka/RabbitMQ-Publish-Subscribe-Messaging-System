using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace ConsumerEurope
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            string exchangeName = "continents-topic-exchange";
            string europeQueue = "europe_queue";

            channel.ExchangeDeclare(exchange: exchangeName,
                                    type: ExchangeType.Topic,
                                    durable: false,
                                    autoDelete: false,
                                    arguments: null);

            channel.QueueDeclare(queue: europeQueue,
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            channel.QueueBind(queue: europeQueue,
                              exchange: exchangeName,
                              routingKey: "continent.europe");

            int europeCount = 0;

            var europeConsumer = new EventingBasicConsumer(channel);
            europeConsumer.Received += (model, ea) =>
            {
                europeCount++;
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                Console.WriteLine($"[EUROPE #{europeCount}] {message}");
            };

            channel.BasicConsume(queue: europeQueue,
                                 autoAck: true,
                                 consumer: europeConsumer);

            Console.ReadLine();
        }
    }
}
