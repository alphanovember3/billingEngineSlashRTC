import { workerData, parentPort } from 'worker_threads';

// Function to calculate the billing data

const dataCalculate =(arr)=>{

    for(let data of arr){

        if(data.type === "disposecall"){

            if(data.mode_of_calling === "Inbound"){
                inboundConnectedCallCount++;
                
                inboundConnectedTotalSecondsUsage += data.agent_talktime_sec;
                inboundDisposePulseCount++;
                inboundDisposeBilledAmount += data.cpc;

            }

            allConnectedCallCount++;
            allConnectedTotalSecondsUsage += data.agent_talktime_sec;
            allDisposePulseCount++;
            allDisposeBilledAmount += data.cpc;

        }

        else if(data.type === "auto_drop"){
            
            dropCallcount++;
            dropTotalSecondsUsage += data.in_queue_time;
            dropPulseCount++;
            dropBilledAmount = 0;

        }

        else if(data.type === "auto_fail"){
            
            failCallcount++;
            failTotalSecondsUsage += data.totalDurationSeconds;
            failPulseCount++;
            failBilledAmount = 0;

        }

        else if(data.type === "inbound_drop"){
            
            inboundDropCallcount++;
            inboundDropTotalSecondsUsage += data.in_queue_time;
            inboundDropPulseCount++;
            inboundDropBilledAmount = 0;

        }

    }
    return true;
} 


const result = dataCalculate(workerData.arr);

// Send result back to main thread
parentPort.postMessage(result);
