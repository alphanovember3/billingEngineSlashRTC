const fetch = require('node-fetch');
const { Worker } = require('worker_threads');
const path = require('path');
const {connectDB} = require('../common/mongo.js')
const workerpath = path.join(__dirname, '../controller/worker.js');

// Create a pool of 10 workers
const WORKER_COUNT = 10;
const workers = [];
const workerQueue = [];

// Store worker availability status
const workerStatus = new Array(WORKER_COUNT).fill(true);

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
for (let i = 0; i < WORKER_COUNT; i++) {
  const worker = new Worker(workerpath);
  workers.push(worker);

  // Handle worker responses
  worker.on('message', (result) => {
    // Update the calculation
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

    // Mark worker as available
    const workerIndex = workers.indexOf(worker);
    workerStatus[workerIndex] = true;

    // Assign new task if queue is not empty
    if (workerQueue.length > 0) {
      const newTask = workerQueue.shift();
      assignTask(worker, i, newTask);
    }
  });

  
  worker.on('error', (error) => {
    console.error(`Worker ${i} error:`, error);
  });

  worker.on("exit", (code) => {
    // if (code !== 0){
    //   handleWorkerExit(worker, code)
    //   console.error(`Worker stopped with exit code ${code}`);
    // } 
  });
}

// Function to assign a task to an available worker
const assignTask = (worker, workerIndex, data) => {
  workerStatus[workerIndex] = false;
  worker.postMessage(data);
};


function handleWorkerExit(worker, code) {
  try {
    const failedTask = workerTasks.get(worker);
    console.error(` Worker crashed. Reassigning task:`, failedTask);

    // Remove the dead worker
    workers.delete(worker);
    workerTasks.delete(worker);

    if (failedTask) {
      taskQueue.unshift(failedTask);
    }

    const newWorker = new Worker(workerPath);
    workers.add(newWorker);
    
    newWorker.on("message", ({ table, rows }) => {

      console.log(`Writing ${rows.length} rows from ${table} to CSV...`);

      try {
        console.log(rows[0])
        rows.map((row)=>{writer.write(row)})
      } catch (error) {
        console.log("error occured in writing csv", error)
      }
      assignNextTask(newWorker);
    });

    newWorker.on("error", (err) => console.error(`Worker error: ${err}`));

    newWorker.on("exit", (code) =>{
      if(code !== 0){
        handleWorkerExit(newWorker, code)
      }
    
    }) 

    // Assign the recovered task to the new worker
    assignNextTask(newWorker);
    
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
  
  const requestBody = { month: 'feb', year: '2025' };

  const controller = new AbortController();
  const timeout = 1200000;

  const timeoutId = setTimeout(() => controller.abort(), timeout);

  (async()=>{
    try {
    process.on('SIGINT', () => {
      console.log('SIGINT received. Aborting API connection...');
      controller.abort();
      process.exit(0);
    });

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestBody),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);
    console.log(response)
    if (!response.ok) {
      throw new Error(`HTTP error! Status: ${response.status}`);
    }

    const decoder = new TextDecoder();
    let buffer = '';

    response.body.on('data', (chunk) => {
      buffer += decoder.decode(chunk, { stream: true });

      const parts = buffer.split('\n');
      buffer = parts.pop();

      if (parts.length >= 1) {
        processData(parts);
      }
    });

    response.body.on('end', () => {
      if (buffer.trim()) {
        try {
          const data = JSON.parse(buffer);
          console.log('Received final buffer data:', data);
        } catch (error) {
          console.error('Error parsing final buffer:', buffer, error);
        }
      }
       setTimeout(async () => {
        console.log('Total object is:', finalInvoice);
        const db= await connectDB();
        const collection = db.collection('billData')
        let response= collection.insert(finalInvoice)
        console.log(response);
        
      }, 5000);
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
})()
