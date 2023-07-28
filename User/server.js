const express = require('express');
const amqp = require("amqplib/callback_api");
const port = 3001;

const app = express();

app.get("/users", (req, res) => {
    res.send("Message from User service");

    let data = {
        id: 1,
        name: "John",
        age: 25
    }

    amqp.connect('amqp://localhost', function (err, conn) {
        conn.createChannel(function(err, cha){
            const queue = 'message_queue_user';
            const msg = JSON.stringify(data);
            cha.assertQueue(queue, { durable: false});
            cha.sendToQueue(queue, Buffer.from(msg));
            console.log(`Sent ${msg} to ${queue}`);
        })
    })
})

app.listen(port, () => {
    console.log("User service is listening on port:" + port);
});