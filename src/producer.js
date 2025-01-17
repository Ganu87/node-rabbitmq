const amqp = require('amqplib');

const queue = "r-test";


async function sendMessage(data) {
    try {
        console.log(data);
        if (!data) {
            throw new Error("Data is required for sending a message");
        }

        console.log("Connecting to RabbitMQ...");
        const connection = await amqp.connect("amqp://localhost");
        console.log("Connected to RabbitMQ");

        const channel = await connection.createChannel();
        console.log("Channel created");

        //const queue = "r-test"; // Ensure this matches your queue name
        await channel.assertQueue(queue);
        console.log(`Queue "${queue}" asserted`);

        channel.sendToQueue(queue, Buffer.from(JSON.stringify(data)));
        console.log("Message sent:", data);

        await channel.close();
        await connection.close();
        console.log("Connection closed");
    } catch (err) {
        console.error("Error sending message:", err);
    }
}



// async function sendMessage(message) {
    
//     const connection = await amqp.connect("amqp://localhost");
//     const channel = await connection.createChannel();

//     await channel.assertQueue(queue,Buffer.from(message));

//     console.log("Message sent : ",message);

//     await channel.close();

//     await connection.close();
// }

module.exports = {sendMessage};