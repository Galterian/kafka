
const axios = require('axios');
const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');
const moment = require('moment');

var qs = require('qs');
const { exit } = require('process');
var data = qs.stringify({
  'client_id': '735fr4lpcgcfsb0bmlbjl3pn8c',
  'client_secret': 'rnibqne6m43gnm2ja708vhahghh7p20uvfei7tfliuarv2p6qv1',
  'grant_type': 'client_credentials',
});

var config = {
  method: 'post',
  url: 'https://899ce57258d85f4caf87d3512fc4d819.auth.us-east-1.amazoncognito.com/oauth2/token',
  headers: { 
    'Content-Type': 'application/x-www-form-urlencoded'
  },
  data : data
};

async function getToken(){
  let token = await axios(config)
  .then((response) => {
      return response.data.access_token;
  })
  .catch((error) => {
      console.log(error);
  });
  return token;
}




async function main() {
    const kafka = new Kafka({
        clientId: 'producer-app',
        brokers: ['kafka.streamproc.contentmgmt.stag.pib.dowjones.io:9100'],
        retry: {
          initialRetryTime: 100,
          retries: 2
        },
        authenticationTimeout: 10000,
        reauthenticationThreshold: 10000,
        ssl: true,        
        sasl: {
          mechanism: 'oauthbearer',
          oauthBearerProvider: async () => {
            const token = await getToken() //this is the function to get token
            return {
              value: token
            }
          }
        },
        refreshThresholdMs: 10000, // Refresh token 10 seconds before it expires
      });    

    const admin = kafka.admin();
    await admin.connect();

    //const topic = `producer_and_consumer_group_${dateTime}`;
    const topic = process.argv[2] || `producer_and_consumer_group_${Date.now()}`;
    // await admin.createTopics({
        
    //     topics: [
    //         {
    //             topic,
    //             numPartitions: 30,
    //             replicationFactor: 3,
    //         },
    //     ],
    // });
    console.log(`Topic created: ${topic}`);
    await admin.disconnect();


    const producer = kafka.producer({
        transactionTimeout: 60000, // Set transaction timeout to 60 seconds
        lingerMs: 100,
        messageTimeout: 10000,
        allowAutoTopicCreation: true,
        createPartitionIfNotExists: true
    });
    
    const { CONNECT, DISCONNECT, REQUEST, REQUEST_TIMEOUT } = producer.events;
    producer.on(CONNECT, (event) => {
        console.log(`Producer connected to Kafka broker ${JSON.stringify(event.payload)}`);
    });
    producer.on(DISCONNECT, (event) => {
        console.log(`Producer disconnected from Kafka broker ${JSON.stringify(event)}`);
    });
    producer.on(REQUEST, (event) => {
        console.log(`Producer request sent: ${JSON.stringify(event.payload)}`);
    });
    producer.on(REQUEST_TIMEOUT, (event) => {
        console.error(`Producer request timed out: ${JSON.stringify(event.payload)}`);  
    });

    await producer.connect();

    //let refreshId = setInterval(async () => {
        const headers = { 'Content-Type': 'application/octet-stream' };
        const contents = fs.readFileSync(path.resolve(__dirname, 'Data/Program.txt'));
        const message = {
            key: `${Date.now()}`,
            value: contents,
            headers,
        };
        try {
            const result = await producer.send({
                topic,
                messages: [message],
                timeout: 30000, // Set a timeout of 30 seconds for the send operation
            });
            console.log(`Message delivered: ${JSON.stringify(result)}`);
        } catch (err) {
            await new Promise(resolve => setTimeout(resolve, 1000)); // Delay before retrying
            console.error(`Error producing message: ${err.message}`);
            if(err.message == 'The producer is disconnected' || err.message == 'Closed connection') {
                console.error('JS Producer is disconnected');
                //clearInterval(refreshId);
            }
        }
    //}, 200);
    const groupId = `test-group_${topic}`;
    const consumerGroupFetchEnded = Array(3).fill(Date.now()); 
    const consumerStarted = Array(3).fill(false); 
    const consumerGroup = [];
    for (let i = 0; i < 3; i++) {
        const consumer = kafka.consumer({ groupId: groupId, allowAutoTopicCreation: true, maxWaitTimeInMs: 5000, maxBytesPerPartition: 1048576, maxBytes: 10485760 });
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: true, autoCommit: true });
        const { HEARTBEAT, CRASH, DISCONNECT, STOP, GROUP_JOIN, 
            REBALANCING, RECEIVED_UNSUBSCRIBED_TOPICS, FETCH_START, FETCH } = consumer.events;
        consumer.on(GROUP_JOIN, (event) => {
            console.log(`Consumer ${i} connected to group ${event.payload.groupId}`);
        });
        consumer.on(HEARTBEAT, () => {
            console.log(`Consumer ${i} heartbeat`);
        });
        consumer.on(CRASH, (event) => {
            console.error(`Consumer ${i} crashed: ${event.payload.error.message}`);
        });
        consumer.on(DISCONNECT, (event) => {   
            console.log(`Consumer ${i} disconnected from group at ${JSON.stringify(event)}`);
        });
        consumer.on(STOP, (event) => {
            console.log(`Consumer ${i} stopped consuming messages from group at ${JSON.stringify(event)}`);
        });
        consumer.on(REBALANCING, (event) => {
            console.log(`Consumer ${i} rebalance event: ${JSON.stringify(event)}`);
        });
        consumer.on(RECEIVED_UNSUBSCRIBED_TOPICS, (event) => {  
            console.log(`Consumer ${i} RECEIVED_UNSUBSCRIBED_TOPICS: ${JSON.stringify(event)}`);
        });
        consumer.on(FETCH_START, (event) => {
            console.log(`Consumer ${i} fetch started: ${JSON.stringify(event.payload)}`);
        });
        consumer.on(FETCH, (event) => {
            const formattedDate = moment(Date.now()).utc().format("YYYY-MM-DDTHH:mm:ss.SSS[Z]");
            console.log(`${formattedDate} Consumer ${i} fetch ended: ${JSON.stringify(event.payload)}`);            
        });

  
        consumerGroup.push(consumer);
    }

    const startTime = Date.now();
    const duration = parseInt(process.argv[6]) || 600000; // Default is 1 minute if not specified
    const disconnectedConsumer = parseInt(process.argv[3]) || 2; // Default to consumer 2 if not specified
    const disconnectedMiliSecond = parseInt(process.argv[4]) || 20000; // Default to true if not specified
    const reconnectedMiliSecond = parseInt(process.argv[5]) || 30000; // Default to true if not specified
    
    let isDisconnected = false;
    let isReconnected = false;
    while (Date.now() - startTime < duration) { // && consumerGroupFetchEnded.some(ended => !ended)
        await new Promise(resolve => setTimeout(resolve, 1000)); // Delay before retrying
        const consumerGroupFetchEndedTimestamp = Math.max(...Object.values(consumerGroupFetchEnded));
        if (Date.now() - consumerGroupFetchEndedTimestamp < 20000) {
            console.log(`Consumer group active consumers still consumes since ${(Date.now() - consumerGroupFetchEndedTimestamp)/1000} seconds`);
            //break; // Skip processing if the group is empty
        } else {
            console.log(`Consumer group active consumers no longer consumes since ${(Date.now() - consumerGroupFetchEndedTimestamp)/1000} seconds`);
            break; // Skip processing if the group is empty
        }
        await Promise.all(
            consumerGroup.map(async (consumer, idx) => {
                try {
                    const groupInfo = await consumer.describeGroup({ groupId });
                    console.log("Consumer group state:", groupInfo);

                    if( consumerStarted[idx] == false) {
                        await consumer.run({
                            autoCommit: true,
                            eachMessage: async ({ message }) => {
                                const formattedDate = moment(Date.now()).utc().format("YYYY-MM-DDTHH:mm:ss.SSS[Z]");
                                console.log(`${formattedDate} Consumer ${idx} Consumed message: ${message.value.toString().substring(0, 10)}...`); // Log first 100 characters of the message
                                consumerGroupFetchEnded[idx] = Date.now(); // Mark fetch ended for this consumer    
                                consumerStarted[idx] = true;
                            }
                        });
                    }
                    if(Date.now() - startTime >= disconnectedMiliSecond && 
                    consumer == consumerGroup[disconnectedConsumer] &&
                    isDisconnected == false) {
                        consumer.disconnect();
                        isDisconnected = true;
                        console.log(`Consumer ${idx} has disconnected`);
                    }
                    if(Date.now() - startTime >= reconnectedMiliSecond && 
                    consumer == consumerGroup[disconnectedConsumer] &&
                    isDisconnected == true && isReconnected == false) {
                        // await consumerGroup[0].pause();
                        // await consumerGroup[1].pause();
                        await consumer.connect();
                        //await consumer.seek({ topic, partition: 0, offset: '0' });
                        // await consumer.subscribe({ topic, fromBeginning: true });
                        // await consumerGroup[0].resume();
                        // await consumerGroup[1].resume();
                        isReconnected = true;
                        console.log(`Consumer ${idx} has rejoined`);
                    }

                } catch (err) {
                    console.error(`Error consuming message: ${err.message}`);
                }
            })
        );
    }
    await new Promise(resolve => setTimeout(resolve, 10000)); // Delay before stopping consumers
    await Promise.all(consumerGroup.map((consumer) => consumer.disconnect()));
    await producer.disconnect();
    console.log('All consumers and producer disconnected successfully.');
    const formattedDate = moment(Date.now()).utc().format("YYYY-MM-DDTHH:mm:ss.SSS[Z]");
    console.log(`Exiting the application at ${formattedDate}`);
    // clearInterval(refreshId);
    exit(0);
}

main().catch((err) => console.error(`Error in main: ${err.message}`));
