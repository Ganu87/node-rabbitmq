const amqp = require('amqplib');

const queue = "r-test";

async function startConsumer() {
    
    const connection = await amqp.connect("amqp://localhost");

    const channel = await connection.createChannel();

    await channel.assertQueue(queue);

    console.log("Counsumer started");
    console.log("Waitig for messages .......");

    channel.consume(queue,(msg) =>{
        console.log("Received : ",msg.content.toString());
        channel.ack(msg);
    })
}

module.exports = {startConsumer};