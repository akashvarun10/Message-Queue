const {kafka} = require('./client')

async function init(){
    const admin = kafka.admin();
    console.log('Connecting...');
    admin.connect()
    console.log('Connected');

    console.log('Creating topic [rider-updates]');
    admin.createTopics({
        topics: [
        {
            topic: 'rider-updates',
            numPartitions: 2,
        },
    ],
    });
    console.log('Created topic [rider-updates]');
    console.log("Disconnecting...");
    await admin.disconnect();
}

init();
