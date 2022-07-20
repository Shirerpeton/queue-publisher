import client, { Connection, Channel } from 'amqplib'
import { Command, OptionValues } from 'commander';
import { readFileSync } from 'fs'

const program = new Command();

program
    .name('queue-publisher')
    .description('simple command line script to publish messages to queue')
    .version('1.0.0');

program
    .option('-c, --connection-string <connection-string>', 'connection string to rabbitmq server', 'amqp://localhost:5672')
    .option('-q, --queue <queue>', 'queue name', 'queue')
    .option('-f, --file <file>', 'csv file with messages publish', 'file')
    .option('-d, --delay <delay>', 'delay in milliseconds between each message', '0');

program.parse(process.argv);
const options: OptionValues = program.opts();
const connectionString: string = options.connectionString;
const queueName: string = options.queue;
const file: string = options.file;
const delay: number = parseInt(options.delay);

const closeChannel = async (channel: Channel): Promise<void> => {
    if(channel) await channel.close();
}

const closeConnection = async (connection: Connection): Promise<void> => {
    if(connection) await connection.close();
}

const readCSVFile = (file: string): string[] => {
    const csvFile = readFileSync(file, 'utf8');
    return csvFile.split('\n');
}

const messages: string[] = readCSVFile(file);

const publishMessage = async (channel: Channel, queueName: string, message: string, delay: number): Promise<void> => {
    await channel.sendToQueue(queueName, Buffer.from(message));
    console.log('Message published: ' + message);
    await new Promise<void>(resolve => setTimeout(() => resolve(), delay));
}

(async (): Promise<void> => {
    const connection: Connection = await client.connect(connectionString);
    
    const channel: Channel = await connection.createChannel();
    await channel.assertQueue(queueName);
    
    for(const message in messages) {
        await publishMessage(channel, queueName, message, delay);
    }

    await closeChannel(channel);
    await closeConnection(connection);
    return;
})();

