// import { parentPort } from 'worker_threads';
console.time("within a worker")
// console.log(`total heap before starting operations: ${(process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2)} MB`);
const {parentPort}= require('worker_threads')
// Function to process data and return calculations
const dataCalculate = (dataChunk) => {
// console.log(`total heap before starting operations: ${(process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2)} MB`);

  let invoice = {
    outboundInfo: {
      connectedCalls: {
        premiumDID: {totalCallCount: 0,totalSecUsage: 0,totalPulseCount: 0,},
        specialDID: {totalCallCount: 0,totalSecUsage: 0,totalPulseCount: 0,},
        virtualDID: {totalCallCount: 0,totalSecUsage: 0,totalPulseCount: 0,TotalBilledAmount: 0,},
      },
      dropCalls: {totalCallCount: 0,totalSecUsage: 0,totalPulseCount: 0,TotalBilledAmount: 0,},
      failedCalls: {totalCallCount: 0,totalSecUsage: 0,totalPulseCount: 0,TotalBilledAmount: 0,},
    },
    inboundInfo: {
      connectedCalls: {totalCallCount: 0,totalSecUsage: 0,totalPulseCount: 0,TotalBilledAmount: 0,},
      missedCalls: {totalCallCount: 0,totalSecUsage: 0,totalPulseCount: 0,TotalBilledAmount: 0,},
    },
  };
  // console.log(`total heap after worker finishing operations: ${(process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2)} MB`);

  try {
    const dataBatch = JSON.parse(dataChunk);
    
    // console.log('SIZE OF ARR IS:', dataBatch.length);
    for (let data of dataBatch) {
      // console.time("time taken for a row")
      try {
        
        if (data.type == 'disposecall') {
          if (data.mode_of_calling !== 'Inbound') {
            // Outbound
            if (data.agent_talktime_sec > 0) {
              // connected
              if (
                data.did_number.startsWith('+91079') || data.did_number.startsWith('079')) {
                  // premium
                  invoice.outboundInfo.connectedCalls.premiumDID.totalCallCount += 1;
                  invoice.outboundInfo.connectedCalls.premiumDID.totalSecUsage += data.agent_talktime_sec;
                  invoice.outboundInfo.connectedCalls.premiumDID.totalPulseCount += data.cpc / 60;
                } else if (data.did_number.startsWith('+91924') || data.did_number.startsWith('924')) {
                  // special
                  invoice.outboundInfo.connectedCalls.specialDID.totalCallCount += 1;
                  invoice.outboundInfo.connectedCalls.specialDID.totalSecUsage += data.agent_talktime_sec;
                  invoice.outboundInfo.connectedCalls.specialDID.totalPulseCount += data.cpc / 60;
                } else {
                  // virtual
                  invoice.outboundInfo.connectedCalls.virtualDID.totalCallCount += 1;
                  invoice.outboundInfo.connectedCalls.virtualDID.totalSecUsage += data.agent_talktime_sec;
                  invoice.outboundInfo.connectedCalls.virtualDID.totalPulseCount += data.cpc / 60;
                  invoice.outboundInfo.connectedCalls.virtualDID.TotalBilledAmount += data.cpc;
                }
              } else {
                // drop
                invoice.outboundInfo.dropCalls.totalCallCount += 1;
                invoice.outboundInfo.dropCalls.totalSecUsage += data.ringing_time_sec_updated;
              }
            } else {
              // Inbound
              if (data.agent_talktime_sec > 0) {
                // Inbound Connected
                invoice.inboundInfo.connectedCalls.totalCallCount += 1;
                invoice.inboundInfo.connectedCalls.totalSecUsage += data.agent_talktime_sec;
                invoice.inboundInfo.connectedCalls.totalPulseCount += data.cpc / 60;
                invoice.inboundInfo.connectedCalls.TotalBilledAmount += data.cpc;
              } else {
                // Inbound Missed Calls
                invoice.inboundInfo.missedCalls.totalCallCount += 1;
              }
            }
          } else if (data.type === 'auto_drop_cdr') {
            // autodrop
            invoice.outboundInfo.dropCalls.totalCallCount += 1;
          } else if (data.type === 'auto_failed_calls') {
            // autofailed
            invoice.outboundInfo.failedCalls.totalCallCount += 1;
          } else {
            // inbound_drop_cdr
            invoice.inboundInfo.missedCalls.totalCallCount += 1;
          }
          
        } catch (innerError) {
          console.error('Error processing data entry:', innerError);
        }
        // console.timeEnd("time taken for a row")
      }
      
    } catch (error) {
      console.error('Error parsing input data:', error);
    }
    
    return invoice;
  };
  
  // Listen for data from parent
  parentPort.on('message', async (dataChunk) => {
    // console.time("time taken for a batch")
    // console.log(`total heap before starting operations: ${(process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2)} MB`);
    try {
      const result = await dataCalculate(dataChunk);
      parentPort.postMessage(result);
    } catch (error) {
      console.error('Error processing data chunk:', dataChunk, error);
    }
    // console.log(`total heap after worker finishing operations: ${(process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2)} MB`);
    // console.timeEnd("time taken for a batch")
  });
  // console.log(`total heap after worker finishing operations: ${(process.memoryUsage().heapTotal / 1024 / 1024).toFixed(2)} MB`);
  console.timeEnd("within a worker")