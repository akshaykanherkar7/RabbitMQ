const express = require('express');
const amqp = require("amqplib/callback_api");
const port = 3000;

const app = express();

app.get("/products", (req, res) => {
    amqp.connect("amqp://localhost", function(err, conn){
        conn.createChannel(function(err, ch){
            const queue = "message_queue_user";
            console.log("Waiting for the message from queue");
            ch.consume(queue, async function(msg){
                console.log(`Message: ${msg.content.toString()}`);
                await res.send(msg.content.toString());
            }, { noAck: true })
        })
    })
})

app.listen(port, () => {
    console.log("Product service is listening on port:" + port);
});