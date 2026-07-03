using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace ConsumerAsia
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            string exchangeName = "continents-topic-exchange";
            string asiaQueue = "asia_queue";

            channel.ExchangeDeclare(exchange: exchangeName,
                                    type: ExchangeType.Topic,
                                    durable: false,
                                    autoDelete: false,
                                    arguments: null);

            channel.QueueDeclare(queue: asiaQueue,
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            channel.QueueBind(queue: asiaQueue,
                              exchange: exchangeName,
                              routingKey: "continent.asia");

            int asiaCount = 0;

            var asiaConsumer = new EventingBasicConsumer(channel);
            asiaConsumer.Received += (model, ea) =>
            {
                asiaCount++;
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                Console.WriteLine($"[ASIA #{asiaCount}] {message}");
            };

            channel.BasicConsume(queue: asiaQueue,
                                 autoAck: true,
                                 consumer: asiaConsumer);

            Console.ReadLine();
        }
    }
}
