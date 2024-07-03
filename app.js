const express = require('express');
const bodyParser = require('body-parser');
const mqtt = require('mqtt');
const { v4: uuidv4 } = require('uuid');

const app = express();
const port = 6001; // Port for HTTP server
const MQTT_BROKER = 'mqtt://27.71.235.82';
const MQTT_TOPIC_RCV = 'api/oracle/rcv/#';
const MQTT_TOPIC_SND = 'api/oracle/snd';
const TIMEOUT = 30000; // Timeout in milliseconds

let receivedData = {};

// MQTT client setup
const mqttClient = mqtt.connect(MQTT_BROKER);

mqttClient.on('connect', function () {
    console.log('Connected to MQTT broker');
    mqttClient.subscribe(MQTT_TOPIC_RCV, function (err) {
        if (err) {
            console.error('Error subscribing to MQTT topic:', err);
        }
    });
});

mqttClient.on('message', function (topic, message) {
    try {
        const key = topic.split('/').pop();
        console.log("message " + key);
        const data = JSON.parse(message.toString());
        console.log("data " + message.toString());
        if (key) {
            if (receivedData[key]) {
                receivedData[key] = { ...receivedData[key], ...data };
            } else {
                receivedData[key] = data;
            }
            console.log('Received data:', receivedData[key]);
        }
    } catch (error) {
        console.error('Error processing MQTT message:', error);
    }
});

// Express middleware setup
app.use(express.json());

// Routes
app.post('/:DB/:PROCEDURE', (req, res) => {
    console.log(req.params.DB);
    console.log(req.params.PROCEDURE);
    console.log(req.body);
    const key = uuidv4();
    console.log("key " + key);
    data = req.params.DB + '.' + req.params.PROCEDURE
    console.log("data " + data);
    try {
        const { PROCEDURE, ...data } = req.body;
        const message = JSON.stringify(data);
        console.log(message);
        //const mqttTopic = `${MQTT_TOPIC_SND}/${data}/${key}`;
        const mqttTopic = MQTT_TOPIC_SND + '/' + req.params.DB + '.' + req.params.PROCEDURE +'/' + key
        console.log("mqttTopic " + mqttTopic);
        mqttClient.publish(mqttTopic, message, function (err) {
            if (err) {
                console.error('Error publishing MQTT message:', err);
                res.status(500).json({ error: 'Internal Server Error' });
            }
        });

        const startTime = Date.now();
        const interval = setInterval(() => {
            if (receivedData[key]) {
                const responseData = receivedData[key];
                delete receivedData[key];
                clearInterval(interval);
                res.status(200).json(responseData);
            } else if (Date.now() - startTime > TIMEOUT) {
                clearInterval(interval);
                res.status(400).json({ Status: 'Bad Request' });
            }
        }, 100);
    } catch (error) {
        console.error('Error handling request:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});



// Start server
app.listen(port, () => {
    console.log(`Server running on port ${port}`);
});
