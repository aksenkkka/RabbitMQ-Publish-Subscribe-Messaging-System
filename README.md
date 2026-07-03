# RabbitMQ Publish/Subscribe Messaging System

A C# implementation of an asynchronous messaging system using **RabbitMQ** and the **Publish/Subscribe** messaging pattern. The project demonstrates message routing with **Topic** and **Direct** exchanges, allowing publishers to distribute messages selectively to multiple consumers.

The application simulates continent-based message routing, where each consumer subscribes to specific routing keys while an additional consumer receives messages from all topics.

---

## Features

* RabbitMQ integration
* Topic Exchange routing
* Direct Exchange routing
* Multiple independent consumers
* Publisher application
* Routing key filtering
* Real-time asynchronous messaging
* Message counters for each consumer

---

## Technologies

* C#
* .NET
* RabbitMQ
* RabbitMQ.Client

---

## Messaging Architecture

The application consists of:

* **Publisher**

  * Sends messages to a RabbitMQ Topic Exchange.
  * Randomly selects a continent.
  * Publishes messages every two seconds.

* **Consumers**

  * Europe Consumer
  * Asia Consumer
  * North America Consumer
  * All Continents Consumer

Each consumer listens only to the routing keys it is interested in.

---

## Topic Exchange

Exchange name:

```text
continents-topic-exchange
```

Routing keys:

```text
continent.europe
continent.asia
continent.north_america
```

Queue bindings:

| Queue                | Routing Key             |
| -------------------- | ----------------------- |
| europe_queue         | continent.europe        |
| asia_queue           | continent.asia          |
| north_america_queue  | continent.north_america |
| all_continents_queue | continent.#             |

The wildcard routing key:

```text
continent.#
```

allows the **ConsumerAll** application to receive every message published to the exchange.

---

## Direct Exchange Example

The project also includes an example using a **Direct Exchange**.

Exchange:

```text
direct_logs
```

Supported routing keys:

* info
* warning
* error

A consumer subscribes to all three log levels and receives every matching message.

---

## Publisher

The publisher:

* connects to RabbitMQ
* creates the Topic Exchange
* declares queues
* binds queues to routing keys
* randomly selects a continent
* publishes a message every two seconds

Example message:

```text
Message for EUROPE
```

Console output:

```text
Message type continent.europe is sent to EUROPE [N:12]
```

---

## Consumers

### Europe Consumer

Receives only:

```text
continent.europe
```

Example:

```text
[EUROPE #5] Message for EUROPE
```

---

### Asia Consumer

Receives only:

```text
continent.asia
```

Example:

```text
[ASIA #3] Message for ASIA
```

---

### North America Consumer

Receives only:

```text
continent.north_america
```

Example:

```text
[NORTH_AMERICA #7] Message for NORTH AMERICA
```

---

### All Continents Consumer

Receives every published message using:

```text
continent.#
```

Example:

```text
Consumer received: Message type (continent.europe) from publisher [N:10]
```

---

## Running the Project

### 1. Start RabbitMQ

Ensure a RabbitMQ server is running locally.

Default connection:

```text
Host: localhost
Port: 5672
```

---

### 2. Start Consumers

Run one or more consumer applications:

* ConsumerEurope
* ConsumerAsia
* ConsumerNorthAmerica
* ConsumerAll

---

### 3. Start Publisher

Run the Publisher application.

Messages will begin appearing in the subscribed consumer windows.

---
