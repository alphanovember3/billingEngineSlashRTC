const fetch = require('node-fetch');
const https = require('https');
const { Worker } = require('worker_threads');
const path = require('path');
const {connectDB} = require('../common/mongo.js')
const workerpath = path.join(__dirname, '../controller/worker.js');
const cron = require('node-cron')
process.env.UV_THREADPOOL_SIZE = 4;
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
  businessID :0,
  clientName:"",
  month:"",
  year:0,
  license : {},
  premiumDidCount:0,
  premiumDidList:[],
  specialDidCount:0,
  specialDidList:[],
  virtualDidCount:0,
  virtualDidList:[],
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

const getLicenceCount =async (clientDetails)=>{
  try {
    const response = await fetch(clientDetails.licenceApi, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${clientDetails.token}`,
        'Content-Type': 'application/json',
      },
      signal: controller.signal,
    });
    const result = await response.json();
    if (result.success){
      for(license of result.status){
        if(license.id==1){finalInvoice.license.agent = license.value}
        if(license.id==60){finalInvoice.license.admin = license.value}
        if(license.id==61){finalInvoice.license.supervisor = license.value}
        if(license.id=62){finalInvoice.license.teamleader = license.value}
      }
    }else{
      console.log("Not getting proper response")
    }
  } catch (error) {
    console.log("error getting license count: ", error)
  }
}

const getDidInfo = async(clientDetails)=>{
  try {
    const response = await fetch(clientDetails.didInfoApi, {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${clientDetails.token}`,
        'Content-Type': 'application/json',
      },
      signal: controller.signal,
    });
    const result = await response.json() 
    if (result.success){
      const didInfo = result.status.rows;
      for(did of didInfo){
        if(did.gateway_caller_id.startsWith('+91079') || did.gateway_caller_id.startsWith('079')){
          finalInvoice.premiumDidCount +=1;
          finalInvoice.premiumDidList.push(did.gateway_caller_id)
        }else if(did.gateway_caller_id.startsWith('+91924') || did.gateway_caller_id.startsWith('924')){
          finalInvoice.specialDidCount +=1;
          finalInvoice.specialDidList.push(did.gateway_caller_id)
        }else{
          finalInvoice.virtualDidCount +=1;
          finalInvoice.virtualDidList.push(did.gateway_caller_id)
        }
      }
    }else{
      console.log("Not getting proper response")
    }
  } catch (error) {
    console.log("error getting Did Information: ", error)
  }
}

// Initialize worker pool
console.log(`total heap memory before before creating workers: ${(process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2)} MB`);
for (let i = 0; i < WORKER_COUNT; i++) {
  const worker = new Worker(workerpath);
  workers.push(worker);

  // Handle worker responses
  worker.on('message', (result) => {
    // Update the calculation
    updatData(result)

    const workerIndex = workers.indexOf(worker);
    workerStatus[workerIndex] = true;

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

const processData = (dataChunk) => {
  const availableWorkerIndex = workerStatus.findIndex((status) => status === true);

  if (availableWorkerIndex !== -1) {
    assignTask(workers[availableWorkerIndex], availableWorkerIndex, dataChunk);
  } else {
    console.log('All workers are busy');
    workerQueue.push(dataChunk);
  }
};

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
      if(!clientDetails){

      }
        finalInvoice.businessID = clientDetails.businessId;
        finalInvoice.clientName = clientDetails.clientName;
        const licenseCount = getLicenceCount(clientDetails);
        const didInfo = getDidInfo(clientDetails);
        let requestBody = getRequestBody();
        finalInvoice.month = requestBody.month;
        finalInvoice.year = requestBody.year;
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
        
      clearTimeout(timeoutId);

      // if (!response.ok) {
      //   throw new Error(`HTTP error! Status: ${response.status}`);
      // }


      let buffer = '';

      console.time("time for fetching data streams")
      let batchCompleted = true
      let count = 0
      response.body.setEncoding('utf8');
      response.body.on('data', (chunk) => {
        
        if(batchCompleted){
          // console.time("processing 1 batch")
          batchCompleted=false
        }

        buffer+= chunk
        let newlineIndex;
        while ((newlineIndex = buffer.indexOf('\n')) >= 0) {
          // console.log(count++);
          const batch = buffer.slice(0, newlineIndex);
          buffer = buffer.slice(newlineIndex + 1);
          if (batch.length>0){
            console.log(count++);
              processData(batch);
              // console.timeEnd("processing 1 batch")
            } 
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

function handleWorkerExit(worker, code) {
  try {
    const failedTask = workerTasks.get(worker);
    const workerIndex = workers.indexOf(worker);
    
    // Validate indices and tasks
    if (workerIndex === -1) {
      console.error('Worker not found in workers array');
      return;
    }

    // Log the crash with more context
    console.error(`Worker ${workerIndex} crashed with exit code ${code}`);
    console.error('Failed task:', failedTask ? 'Task found' : 'No task found');
    
    // Clean up the crashed worker
    workers[workerIndex] = null;  // Mark as null instead of deleting
    workerTasks.delete(worker);
    
    // Requeue the failed task if it exists
    if (failedTask) {
      workerQueue.unshift(failedTask);
    }
    
    // Create new worker
    const newWorker = new Worker(workerpath);
    workers[workerIndex] = newWorker;
    workerStatus[workerIndex] = true;
    
    // Set up event handlers for new worker
    newWorker.on("message", (result) => {
      updateData(result);
      workerStatus[workerIndex] = true;
      
      if (workerQueue.length > 0) {
        const newTask = workerQueue.shift();
        assignTask(newWorker, workerIndex, newTask);
      }
    });

    newWorker.on("error", (err) => {
      console.error(`New worker ${workerIndex} error:`, err);
    });

    newWorker.on("exit", (code) => {
      if (code !== 0) {
        handleWorkerExit(newWorker, code);
      }
    });

  } catch (error) {
    console.error("Critical error in handleWorkerExit:", error);
    console.error("Stack trace:", error.stack);
    
    // Attempt to recover the system state
    try {
      // Clean up any dangling references
      const workerIndex = workers.indexOf(worker);
      if (workerIndex !== -1) {
        workers[workerIndex] = null;
        workerStatus[workerIndex] = true;
      }
      workerTasks.delete(worker);
    } catch (cleanupError) {
      console.error("Failed to clean up after error:", cleanupError);
    }
  }
}