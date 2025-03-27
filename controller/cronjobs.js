const fetch = require('node-fetch');
const https = require('https');
const { Worker } = require('worker_threads');
const path = require('path');
const {connectDB} = require('../common/mongo.js')
const workerpath = path.join(__dirname, '../controller/worker.js');
const cron = require('node-cron')
process.env.UV_THREADPOOL_SIZE = 4;
const WORKER_COUNT = 10;
const controller = new AbortController();
const timeout = 1200000;
const timeoutId = setTimeout(() => controller.abort(), timeout);
const { v4: uuidv4 } = require('uuid');

// const final = {
//   businessID :0,
//   clientName:"",
//   month:"",
//   year:0,
//   license : {},
//   premiumDidCount:0,
//   premiumDidList:[],
//   specialDidCount:0,
//   specialDidList:[],
//   virtualDidCount:0,
//   virtualDidList:[],
//   outboundInfo: {
//     connectedCalls: {
//       premiumDID: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0 },
//       specialDID: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0 },
//       virtualDID: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0, TotalBilledAmount: 0 },
//     },
//     dropCalls: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0, TotalBilledAmount: 0 },
//     failedCalls: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0, TotalBilledAmount: 0 },
//   },
//   inboundInfo: {
//     connectedCalls: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0, TotalBilledAmount: 0 },
//     missedCalls: { totalCallCount: 0, totalSecUsage: 0, totalPulseCount: 0, TotalBilledAmount: 0 },
//   },
//   };


  const insertBillData = async(clientDetails)=>{
    const workers = [];
    const workerQueue = [];
    let workerTasks = new Map();
    const workerStatus = new Array(WORKER_COUNT).fill(true);
    let hasInserted = false;
    let finalInvoice = {
                      businessID :0,
                      clientName:"",
                      month:"",
                      year:0,
                      createdAt: Date.now(),
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

    function updateData(result){
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
            if(license.id==62){finalInvoice.license.teamleader = license.value}
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

    const cleanupWorkers = async () => {
      try {
        // Wait for all tasks in queue to be processed
        while (workerQueue.length > 0) {
          await new Promise(resolve => setTimeout(resolve, 100));
        }
    
        // Wait for all workers to complete their current tasks
        await Promise.all(workers.map(worker => {
          return new Promise((resolve) => {
            const workerIndex = workers.indexOf(worker);
            if (workerStatus[workerIndex]) {
              worker.terminate();
              resolve();
            } else {
              
              const timeout = setTimeout(() => {
                worker.terminate();
                resolve();
              }, 1000); 
    
              worker.once('message', () => {
                clearTimeout(timeout);
                worker.terminate();
                resolve();
              });
            }
          });
        }));
    
        // Clear all arrays and maps
        workers.length = 0;
        workerQueue.length = 0;
        workerTasks.clear();
        workerStatus.fill(true);
        
        console.log('All workers cleaned up successfully');
      } catch (error) {
        console.error('Error during worker cleanup:', error);
      }
    };

    return new Promise(async (resolve, reject)=>{

      const insertInvoice = async ()=>{
        console.log('Total object is:', finalInvoice);
        try {
          finalInvoice._id = uuidv4()
          const db= await connectDB();
          const collection = db.collection('billData')
          let response = await collection.insertOne(finalInvoice)
          cleanupWorkers()
          resolve(response)
        } catch (error) {
          console.log("error occured in inserting data to mongo: ",error)
          reject(error)
        }
      }


      // console.log(`total heap memory before before creating workers: ${(process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2)} MB`);
      for (let i = 0; i < WORKER_COUNT; i++) {
        const worker = new Worker(workerpath);
        workers.push(worker);
      
        worker.on('message',async (result) => {
          updateData(result)
        
          const workerIndex = workers.indexOf(worker);
          workerStatus[workerIndex] = true;
        
          if (workerQueue.length > 0) {
            const newTask = workerQueue.shift();
            workerTasks.set(worker,newTask);
            assignTask(worker, i, newTask);
          }
        
          if(!hasInserted && workerStatus.filter(worker => worker===true).length===WORKER_COUNT){
            await insertInvoice();
            hasInserted = true;
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
        
          workerStatus[workerIndextemp] = true;
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

      try {
        if(!clientDetails){
          reject()
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
          });

          response.body.on('error', (error) => {
            console.error('Stream error:', error);
          });
      } catch (error) {
          cronTasks.unshift(clientDetails);
          console.log("adding failed task to the array", clientDetails)
          console.log("Error occured", error)
          if (error.name === 'AbortError') {
            console.error('Request timed out');
            reject(error)
          } else {
            console.error('Error fetching data:', error);
            reject(error)
        }
      }
    })
  }

const getClientDetails = async ()=>{
  console.log("fetching client");
  try {
    const db= await connectDB();
    const collection =await db.collection('clientDetails')
    let response = await collection.find({}).toArray()
    // console.log("client details fetched: ", response);
    return response;
  } catch (error) {
    console.log("error occured in fetching client details: ", error)
    return {};
  }
}

function getRequestBody(){
  const now = new Date(Date.now());
  const date = now.getDate();
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const month = months[now.getMonth()];
  const year = now.getFullYear();
  return {day:date, month: month.toLowerCase(), year: year };
}

const cronTasks = [];
let isProcessing = false;
setInterval(async () => {
  console.log("checking the tasks");
  while (cronTasks.length > 0 && !isProcessing) {
    isProcessing = true;
    const task = cronTasks.shift();
    let result = await insertBillData(task);
    console.log("tasks pending",cronTasks.length)
    isProcessing = false;
  }
}, 10*1000);


async function scheduletask(){
  const clients = await getClientDetails()
  clients.forEach(client => {
    cron.schedule(client.cronDate, () => {
    cronTasks.push(client);
    console.log(`Task added for client ${client.clientName}`);
    });
  }); 
}
scheduletask()
