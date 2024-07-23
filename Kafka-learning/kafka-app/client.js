const {Kafka} = require('kafkajs');

exports.kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['10.30.1.30:9092']
});