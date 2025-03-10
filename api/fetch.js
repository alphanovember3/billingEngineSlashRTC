import { Worker } from 'worker_threads';

// Sample data array (replace with actual data)
const dataArray = [
    { type: "disposecall", mode_of_calling: "Inbound", agent_talktime_sec: 120, cpc: 60 },
    { type: "auto_drop", in_queue_time: 30 },
    { type: "auto_fail", totalDurationSeconds: 50 },
    { type: "inbound_drop", in_queue_time: 40 },
];

// Initializing variables for calculations
let inboundConnectedCallCount = 0;
let inboundConnectedTotalSecondsUsage = 0;
let inboundDisposePulseCount = 0;
let inboundDisposeBilledAmount = 0;

let allConnectedCallCount = 0;
let allConnectedTotalSecondsUsage = 0;
let allDisposePulseCount = 0;
let allDisposeBilledAmount = 0;

let dropCallcount = 0;
let dropTotalSecondsUsage = 0;
let dropPulseCount = 0;
let dropBilledAmount = 0;

let failCallcount = 0;
let failTotalSecondsUsage = 0;
let failPulseCount = 0;
let failBilledAmount = 0;

let inboundDropCallcount = 0;
let inboundDropTotalSecondsUsage = 0;
let inboundDropPulseCount = 0;
let inboundDropBilledAmount = 0;

// Worker Thread Execution
const worker = new Worker('./worker.js', {
    workerData: {
        arr: dataArray,
        inboundConnectedCallCount,
        inboundConnectedTotalSecondsUsage,
        inboundDisposePulseCount,
        inboundDisposeBilledAmount,
        allConnectedCallCount,
        allConnectedTotalSecondsUsage,
        allDisposePulseCount,
        allDisposeBilledAmount,
        dropCallcount,
        dropTotalSecondsUsage,
        dropPulseCount,
        dropBilledAmount,
        failCallcount,
        failTotalSecondsUsage,
        failPulseCount,
        failBilledAmount,
        inboundDropCallcount,
        inboundDropTotalSecondsUsage,
        inboundDropPulseCount,
        inboundDropBilledAmount
    }
});

// Listen for the message from the worker
worker.on('message', (result) => {
    console.log("Worker Finished Processing:", result);
});

// Listen for errors
worker.on('error', (error) => {
    console.error("Worker Error:", error);
});

// Listen for exit event
worker.on('exit', (code) => {
    if (code !== 0) {
        console.error(`Worker stopped with exit code ${code}`);
    } else {
        console.log("Worker completed successfully.");
    }
});
