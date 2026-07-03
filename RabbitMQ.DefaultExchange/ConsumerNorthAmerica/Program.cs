using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace ConsumerNorthAmerica
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            string exchangeName = "continents-topic-exchange";
            string northAmericaQueue = "north_america_queue";

            channel.ExchangeDeclare(exchange: exchangeName,
                                    type: ExchangeType.Topic,
                                    durable: false,
                                    autoDelete: false,
                                    arguments: null);

            channel.QueueDeclare(queue: northAmericaQueue,
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            channel.QueueBind(queue: northAmericaQueue,
                              exchange: exchangeName,
                              routingKey: "continent.north_america");

            int naCount = 0;

            var naConsumer = new EventingBasicConsumer(channel);
            naConsumer.Received += (model, ea) =>
            {
                naCount++;
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());
                Console.WriteLine($"[NORTH_AMERICA #{naCount}] {message}");
            };

            channel.BasicConsume(queue: northAmericaQueue,
                                 autoAck: true,
                                 consumer: naConsumer);

            Console.ReadLine();
        }
    }
}
