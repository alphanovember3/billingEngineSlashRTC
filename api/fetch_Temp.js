const { Worker } = require("worker_threads");
const fs = require("fs");
const csvWriter = require("csv-write-stream");
const mysql = require("mysql2/promise");

const TABLES = ["table1", "table2", "table3", "table4"];
const BATCH_SIZE = 50000;
const WORKER_COUNT = 10;
const CSV_FILE = "output.csv";

const workerTasks = new Map(); // Stores assigned tasks for recovery

async function getTotalRows(table) {
  const connection = await mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "password",
    database: "your_db",
  });

  const [rows] = await connection.execute(`SELECT COUNT(*) AS total FROM ${table}`);
  await connection.end();
  return rows[0].total;
}

async function createTaskQueue() {
  const taskQueue = [];

  for (const table of TABLES) {
    const totalRows = await getTotalRows(table);
    console.log(`Total rows in ${table}: ${totalRows}`);

    for (let offset = 0; offset < totalRows; offset += BATCH_SIZE) {
      taskQueue.push({ table, limit: BATCH_SIZE, offset });
    }
  }

  return taskQueue;
}

async function startWorkers() {
  const taskQueue = await createTaskQueue();
  let activeWorkers = 0;
  const workers = new Set();

  // Single write stream to prevent corruption
  const fileExists = fs.existsSync(CSV_FILE);
  const writeStream = fs.createWriteStream(CSV_FILE, { flags: "a" });
  const writer = csvWriter({ headers: !fileExists });

  writer.pipe(writeStream);

  function startWorker(worker, task) {
    activeWorkers++;
    workerTasks.set(worker, task);
    worker.postMessage(task);
  }

  function assignNextTask(worker) {
    if (taskQueue.length > 0) {
      const nextTask = taskQueue.shift();
      startWorker(worker, nextTask);
    } else {
      activeWorkers--;
      if (activeWorkers === 0) {
        writer.end();
        console.log("All data written to output.csv successfully!");
      }
    }
  }

  function handleWorkerExit(worker, code) {
    const failedTask = workerTasks.get(worker);
    console.error(` Worker crashed. Reassigning task:`, failedTask);

    // Remove the dead worker
    workers.delete(worker);
    workerTasks.delete(worker);

    if (failedTask) {
      taskQueue.unshift(failedTask); // Re-add task to the front of the queue
    }

    // Spawn a new worker
    const newWorker = new Worker("./worker.js");
    workers.add(newWorker);
    
    newWorker.on("message", ({ table, rows }) => {
      console.log(`Writing ${rows.length} rows from ${table} to CSV...`);
      rows.forEach((row) => writer.write({ table, ...row }));
      assignNextTask(newWorker);
    });

    newWorker.on("error", (err) => console.error(`Worker error: ${err}`));
    newWorker.on("exit", (code) => handleWorkerExit(newWorker, code));

    // Assign the recovered task to the new worker
    assignNextTask(newWorker);
  }

  for (let i = 0; i < WORKER_COUNT; i++) {
    const worker = new Worker("./worker.js");

    worker.on("message", ({ table, rows }) => {
      console.log(`Writing ${rows.length} rows from ${table} to CSV...`);
      rows.forEach((row) => writer.write({ table, ...row }));
      assignNextTask(worker);
    });

    worker.on("error", (err) => console.error(`Worker error: ${err}`));
    worker.on("exit", (code) => handleWorkerExit(worker, code));

    workers.add(worker);
  }

  for (const worker of workers) {
    assignNextTask(worker);
  }
}

startWorkers();
