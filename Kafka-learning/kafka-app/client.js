const {Kafka} = require('kafkajs');

exports.kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['10.20.1.44:9092']
});