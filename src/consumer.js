const amqp = require('amqplib');
const { Pool } = require('pg');

const queue = "r-test";

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'nodejs',
    password: 'postgresql@123',
    port: 5432
})

async function startConsumer() {

    const connection = await amqp.connect("amqp://localhost");

    const channel = await connection.createChannel();

    await channel.assertQueue(queue);

    console.log("Counsumer started");
    console.log("Waitig for messages .......");

    channel.consume(queue, async (data) => {
        if(data){
            
            const message = JSON.parse(data.content.toString());
            console.log("Received : ", message);

            const query=`INSERT INTO RABITM_QUEUE (SOURCE,DATA) VALUES($1,$2)`;

            const values= [message.source,message.data];
            try {
                    await pool.query(query,values);
                    console.log("Data inserted into PostgreSQL:", message);
                    channel.ack(data);
            } catch (err) {
                console.error("Error inserting into PostgreSQL:", err);

            }
        }
        
    })
}

module.exports = { startConsumer };