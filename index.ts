import express, { Request, Response } from "express";
import http from "http";
import { EmitEventRequestBody } from "./types";
import { Kafka } from 'kafkajs';
import { parseArgs } from 'node:util';

const args = parseArgs({options: {
  PORT: {type: "string", default: "8081"},
  GROUPID: {type: "string", default: "test-group-1"},
}});

function getRandomInt(max: number) {
  return Math.floor(Math.random() * max);
}

const app = express();
const server = http.createServer(app);
const port = process.env.PORT || args.values.PORT || 8082;

app.use(express.json());

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092', 'localhost:9092']
})

const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: args.values.GROUPID })

app.get("/health-check", function (req: Request, res: Response) {
  res.json({ status: "alive" });
});

app.post(
  "/emit-event",
  async function (req: Request<any, any, EmitEventRequestBody>, res: Response) {
    const { message } = req.body;
    const key = getRandomInt(2).toString();
    await producer.send({
      topic: 'test-topic',
      messages: [
        { value: message ?? 'Hello KafkaJS user!', key },
      ],
    })
    res.send();
  }
);

server.listen(port, async function () {
  console.log(`Listening on port ${port}`);

  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({topic: 'test-topic', fromBeginning: false})

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log({
          topic,
          partition,
          offset: message.offset,
          value: message.value?.toString(),
        })
    },
  })

});
