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


async function startConsumerBulk() {
    try {
      const connection = await amqp.connect('amqp://localhost');
      const channel = await connection.createChannel();
      await channel.assertQueue(queue);
  
      console.log('Waiting for messages...');
  
      // Array to hold messages for bulk insert
      let messages = [];
      let isProcessing = false; // Prevent starting a new bulk insert while one is in progress
  
      // Consume messages from RabbitMQ
      channel.consume(queue, async (data) => {
        if (data && !isProcessing) {
          try {
            // Mark the process as started to avoid re-triggering
            isProcessing = true;
  
            // Parse the message content
            const message = JSON.parse(data.content.toString());
            console.log('Received message:', message);
  
            // Add message to the messages array
            messages.push([message.source, JSON.stringify(message.data)]);
  
            // Once we have 10 messages, insert into PostgreSQL
            if (messages.length === 10) {
              const query = `
                INSERT INTO RABITM_QUEUE (SOURCE, DATA)
                VALUES ${messages
                  .map((_, index) => `($${index * 2 + 1}, $${index * 2 + 2})`)
                  .join(', ')}
              `;
              const values = messages.flat(); // Flatten the values for bulk insert
  
              console.log('Prepared Query:', query);
              console.log('Values:', values);
  
              // Insert data into PostgreSQL
              try {
                await pool.query(query, values);
                console.log('Bulk data inserted into PostgreSQL');
              } catch (err) {
                console.error('Error inserting into PostgreSQL:', err);
              }
  
              // Acknowledge the message only after the insert is complete
              console.log(`Acknowledging ${messages.length} messages.`);
              channel.ack(data);
              
              // Clear the messages array after insertion
              messages = [];
            }
  
            // Mark the process as done
            isProcessing = false;
          } catch (err) {
            console.error('Error processing message:', err);
          }
        } else {
          console.log('Skipping message acknowledgment, consumer is busy.');
        }
      });
  
    } catch (err) {
      console.error('Error in consumer:', err);
    }
  }






// async function startConsumerBulk() {
//     try {
//         const connection = await amqp.connect("amqp://localhost");
//         const channel = await connection.createChannel();
//         await channel.assertQueue(queue);

//         console.log("Waiting for messages...");
//         const messages = []; // Array to store messages for bulk insertion

//         channel.consume(queue, async (data) => {
//             if (data) {
//                 const message = JSON.parse(data.content.toString());
//                 console.log("Received message:", message);

//                 // Convert DATA to JSON string before pushing to the array
//                 messages.push([message.source, JSON.stringify(message.data)]);

//                 // If you have received 10 messages, perform bulk insert
                
//                     try {
//                         // Log the current state of messages
//                         console.log("Messages array before inserting:", messages);

//                         // Generate the query string
//                         const query = `
//                             INSERT INTO RABBITM_QUEUE (SOURCE, DATA)
//                             VALUES ${messages.map(() => "(?, ?)").join(", ")}
//                         `;

//                         // Log the query for debugging
//                         console.log("Generated Query:", query);

//                         // Flatten the messages array into a single array of values
//                         const values = messages.flat(); // Flatten the array of values

//                         // Log the flattened values
//                         console.log("Flattened Values:", values);

//                         // Insert into PostgreSQL in bulk
//                         await pool.query(query, values);
//                         console.log("Bulk data inserted into PostgreSQL");

//                         // Acknowledge all messages
//                         messages.forEach(() => channel.ack(data));

//                         // Clear the messages array for the next batch
//                         messages.length = 0;
//                     } catch (err) {
//                         console.error("Error inserting into PostgreSQL:", err);
//                     }
                
//             }
//         });
//     } catch (err) {
//         console.error("Error in consumer:", err);
//     }
// }



module.exports = { startConsumer,startConsumerBulk };