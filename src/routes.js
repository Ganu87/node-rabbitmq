const express = require('express');
const producer = require('./producer');
const consumer = require('./consumer');

const router = express.Router();

router.post("/send", async (req, res) => {
    const { details } = req.body;
    try {
        await producer.sendMessage(details);
        res.status(200).send("Message sent to RabbitMQ");
    } catch (err) {
        res.status(500).send({ error: "Error sending message"+err });
    }
});

router.get("/consume", async (req, res) => {
    try {
        await consumer.startConsumer();
        res.status(200).send("Consumer started");
    } catch (err) {
        res.status(500).send({ error: "Error starting consumer"+err });
    }
});

router.get("/consumeBulk", async (req, res) => {
    try {
        await consumer.startConsumerBulk();
        res.status(200).send("Bulk Consumer started");
    } catch (err) {
        res.status(500).send({ error: "Error starting Bulk consumer"+err });
    }
});

module.exports = router;