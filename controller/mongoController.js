const fetch = require('node-fetch');
const { Worker } = require('worker_threads');
const path = require('path');
const {connectDB} = require('../common/mongo.js')
const workerpath = path.join(__dirname, '../controller/worker.js');
const cron = require('node-cron')
process.env.UV_THREADPOOL_SIZE = 8;
// Create a pool of 10 workers
const WORKER_COUNT = 10;
const workers = [];
const workerQueue = [];
let workerTasks = new Map();
const workerStatus = new Array(WORKER_COUNT).fill(true);
const billDate = process.env.BILL_DATE
const controller = new AbortController();
const timeout = 1200000;
const timeoutId = setTimeout(() => controller.abort(), timeout);

let finalInvoice = {
  outboundInfo: {
    connectedCalls: {
      premiumDID: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0 },
      specialDID: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0 },
      virtualDID: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0, TotalBilledAmount: 0 },
    },
    dropCalls: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0, TotalBilledAmount: 0 },
    failedCalls: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0, TotalBilledAmount: 0 },
  },
  inboundInfo: {
    connectedCalls: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0, TotalBilledAmount: 0 },
    missedCalls: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0, TotalBilledAmount: 0 },
  },
};

// Initialize worker pool
console.log(`total heap memory before before creating workers: ${(process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2)} MB`);
for (let i = 0; i < WORKER_COUNT; i++) {
  const worker = new Worker(workerpath);
  workers.push(worker);

  // Handle worker responses
  worker.on('message', (result) => {
    // Update the calculation
    updatData(result)

    // Mark worker as available
    const workerIndex = workers.indexOf(worker);
    workerStatus[workerIndex] = true;

    // Assign new task if queue is not empty
    if (workerQueue.length > 0) {
      const newTask = workerQueue.shift();
      workerTasks.set(worker,newTask);
      assignTask(worker, i, newTask);
    }
  });

  worker.on('error', (error) => {
    console.error(`Worker ${i} error:`, error);
  });

  worker.on("exit", (code) => {
    if (code !== 0){
      handleWorkerExit(worker, code)
      console.error(`Worker stopped with exit code ${code}`);
    }
  });
}
console.log(`total heap memory after creating workers: ${(process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2)} MB`);
// Function to assign a task to an available worker
const assignTask = (worker, workerIndex, data) => {
  workerStatus[workerIndex] = false;
  workerTasks.set(worker,data);
  worker.postMessage(data);
};


function handleWorkerExit(worker, code) {
  try {
    const failedTask = workerTasks.get(worker);
    console.error(` Worker crashed. Reassigning task:`, failedTask.length);

    const workerIndextemp = workers.indexOf(worker);
    // Remove the dead worker
    delete workers[workerIndextemp];
    workerTasks.delete(worker);

    if (failedTask) {
      workerQueue.unshift(failedTask);
    }

    const newWorker = new Worker(workerpath);
    workers[workerIndextemp] = newWorker;
    
    newWorker.on("message", (result) => {

      updateData(result)

      const workerIndex = workers.indexOf(worker);
      workerStatus[workerIndex] = true;

      if (workerQueue.length > 0) {
        const newTask = workerQueue.shift();
        assignTask(worker, workerIndex, newTask);
        workerTasks.set(worker,newTask);
      }
    });

    newWorker.on("error", (err) => console.log(`Worker error: ${err}`));

    newWorker.on("exit", (code) =>{
      if(code !== 0){
        handleWorkerExit(newWorker, code)
      }
    }) 
    
  } catch (error) {
    console.log("worker crash function catch block",error);
  }
 
}

// Function to distribute data to workers
const processData = (dataChunk) => {
  const availableWorkerIndex = workerStatus.findIndex((status) => status === true);

  if (availableWorkerIndex !== -1) {
    assignTask(workers[availableWorkerIndex], availableWorkerIndex, dataChunk);
  } else {
    console.log('All workers are busy');
    workerQueue.push(dataChunk);
  }
};

// Main code
  const url = 'https://l3dev.slashrtc.in/slashRtc/auth/getAllBillingEngine';
  const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6OTMsImFjY2Vzc2xldmVsIjoyLCJhZ2VudE5hbWUiOiJzbGFzaCBhZG1pbiIsImFnZW50VXNlck5hbWUiOiJzbGFzaGFkbWluIiwiZW1haWwiOiIiLCJjcm1faWQiOiJ2YXJ1bi5tZWhuZGlyYXR0YUBiYWphamZpbnNlcnYuaW4iLCJpYXQiOjE3NDAzNzU5MDIsImV4cCI6MTc0Mjk2NzkwMn0.wm2lnh60-VJ91pjgjXyZxtlB8yKQ9V01S4R_WzXmnpI";

 




  // cron.schedule(`0 ${billDate} 9 * * *`,()=>{
    // insertBillData()
  // })

  // getClientDetails()

  const insertBillData = async()=>{
    try {
      process.on('SIGINT', () => {
        console.log('SIGINT received. Aborting API connection...');
        controller.abort();
        process.exit(0);
      });
      console.time("Fetching client details");
      const clientDetails = await getClientDetails()
      console.timeEnd("Fetching client details");
      console.log(clientDetails)
      let requestBody = getRequestBody()
      console.time('Fetching data from API');
      const response = await fetch(clientDetails.clientApi, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${clientDetails.token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
        signal: controller.signal,
      });
      // response.body._readableState.highWaterMark = 1024 * 1024;
      console.timeEnd('Fetching data from API');
      clearTimeout(timeoutId);
      // console.log(response)
      // if (!response.ok) {
      //   throw new Error(`HTTP error! Status: ${response.status}`);
      // }

      const decoder = new TextDecoder();
      let buffer = '';

      console.time("time for fetching data streams")
      let batchCompleted = true
      response.body.on('data', (chunk) => {
        if(batchCompleted){
          console.time("processing 1 batch")
          batchCompleted=false
        }
        // buffer += decoder.decode(chunk, { stream: true });
        buffer += chunk.toString()
        const parts = buffer.split('\n');
        buffer = parts.pop();

        if (parts.length >= 1) {
          batchCompleted=true
          console.timeEnd("processing 1 batch")
          processData(parts);
        }
      });

      response.body.on('end', () => {
          try {
            if(buffer.trim()){
            let parts = buffer.split('\n')
            processData(parts)
            console.log('Received final buffer data');
            }
          } catch (error) {
            console.error('Error parsing final buffer',error);
          }
        console.timeEnd("time for fetching data streams")
        setTimeout(async () => {
          console.log('Total object is:', finalInvoice);
          try {
            console.time("Inserting into MongoDB");
            const db= await connectDB();
            const collection = db.collection('billData')
            let response= await collection.insert(finalInvoice)
            console.timeEnd("Inserting into MongoDB");
          } catch (error) {
            console.log("error occured in inserting data to mongo: ",error)
          }
          console.log(response);
        },5000);
      });

      response.body.on('error', (error) => {
        console.error('Stream error:', error);
      });

    } catch (error) {
      if (error.name === 'AbortError') {
        console.error('Request timed out');
      } else {
        console.error('Error fetching data:', error);
      }
    }
}

const getClientDetails = async ()=>{
  console.log("fetching client");
  try {
    const db= await connectDB();
    const collection =await db.collection('clientDetails')
    let response = await collection.find({}).toArray()
    // console.log("client details fetched: ", response);
    return response[0];
  } catch (error) {
    console.log("error occured in fetching client details: ", error)
    return {};
  }
}

function updatData(result){
  finalInvoice.outboundInfo.connectedCalls.premiumDID.totalCallCount += result.outboundInfo.connectedCalls.premiumDID.totalCallCount;
  finalInvoice.outboundInfo.connectedCalls.premiumDID.totalSecUsage += result.outboundInfo.connectedCalls.premiumDID.totalSecUsage;
  finalInvoice.outboundInfo.connectedCalls.premiumDID.totalPulseCount += result.outboundInfo.connectedCalls.premiumDID.totalPulseCount;

  finalInvoice.outboundInfo.connectedCalls.specialDID.totalCallCount += result.outboundInfo.connectedCalls.specialDID.totalCallCount;
  finalInvoice.outboundInfo.connectedCalls.specialDID.totalSecUsage += result.outboundInfo.connectedCalls.specialDID.totalSecUsage;
  finalInvoice.outboundInfo.connectedCalls.specialDID.totalPulseCount += result.outboundInfo.connectedCalls.specialDID.totalPulseCount;

  finalInvoice.outboundInfo.connectedCalls.virtualDID.totalCallCount += result.outboundInfo.connectedCalls.virtualDID.totalCallCount;
  finalInvoice.outboundInfo.connectedCalls.virtualDID.totalSecUsage += result.outboundInfo.connectedCalls.virtualDID.totalSecUsage;
  finalInvoice.outboundInfo.connectedCalls.virtualDID.totalPulseCount += result.outboundInfo.connectedCalls.virtualDID.totalPulseCount;
  finalInvoice.outboundInfo.connectedCalls.virtualDID.TotalBilledAmount += result.outboundInfo.connectedCalls.virtualDID.TotalBilledAmount;

  finalInvoice.outboundInfo.dropCalls.totalCallCount += result.outboundInfo.dropCalls.totalCallCount;
  finalInvoice.outboundInfo.dropCalls.totalSecUsage += result.outboundInfo.dropCalls.totalSecUsage;
  finalInvoice.outboundInfo.dropCalls.totalPulseCount += result.outboundInfo.dropCalls.totalPulseCount;
  finalInvoice.outboundInfo.dropCalls.TotalBilledAmount += result.outboundInfo.dropCalls.TotalBilledAmount;

  finalInvoice.outboundInfo.failedCalls.totalCallCount += result.outboundInfo.failedCalls.totalCallCount;
  finalInvoice.outboundInfo.failedCalls.totalSecUsage += result.outboundInfo.failedCalls.totalSecUsage;
  finalInvoice.outboundInfo.failedCalls.totalPulseCount += result.outboundInfo.failedCalls.totalPulseCount;
  finalInvoice.outboundInfo.failedCalls.TotalBilledAmount += result.outboundInfo.failedCalls.TotalBilledAmount;

  finalInvoice.inboundInfo.connectedCalls.totalCallCount += result.inboundInfo.connectedCalls.totalCallCount;
  finalInvoice.inboundInfo.connectedCalls.totalSecUsage += result.inboundInfo.connectedCalls.totalSecUsage;
  finalInvoice.inboundInfo.connectedCalls.totalPulseCount += result.inboundInfo.connectedCalls.totalPulseCount;
  finalInvoice.inboundInfo.connectedCalls.TotalBilledAmount += result.inboundInfo.connectedCalls.TotalBilledAmount;

  finalInvoice.inboundInfo.missedCalls.totalCallCount += result.inboundInfo.missedCalls.totalCallCount;
  finalInvoice.inboundInfo.missedCalls.totalSecUsage += result.inboundInfo.missedCalls.totalSecUsage;
  finalInvoice.inboundInfo.missedCalls.totalPulseCount += result.inboundInfo.missedCalls.totalPulseCount;
  finalInvoice.inboundInfo.missedCalls.TotalBilledAmount += result.inboundInfo.missedCalls.TotalBilledAmount;
}

function getRequestBody(){
  const now = new Date(Date.now());
  const date = now.getDate();
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const month = months[now.getMonth()];
  const year = now.getFullYear();
  return {day:date, month: month, year: year };
}
console.time("time taken for total data fetching inserting into mongo")
insertBillData()
console.timeEnd("time taken for total data fetching inserting into mongo")
