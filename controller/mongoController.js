const { MongoClient } = require('mongodb');
const { connectDB } = require("./common/mongo");

// const uri = "your_mongodb_connection_string";
// const client = new MongoClient(uri);

const url = "https://l3dev.slashrtc.in/slashRtc/auth/getAllBillingEngine";
const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6OTMsImFjY2Vzc2xldmVsIjoyLCJhZ2VudE5hbWUiOiJzbGFzaCBhZG1pbiIsImFnZW50VXNlck5hbWUiOiJzbGFzaGFkbWluIiwiZW1haWwiOiIiLCJjcm1faWQiOiJ2YXJ1bi5tZWhuZGlyYXR0YUBiYWphamZpbnNlcnYuaW4iLCJpYXQiOjE3NDAzNzU5MDIsImV4cCI6MTc0Mjk2NzkwMn0.wm2lnh60-VJ91pjgjXyZxtlB8yKQ9V01S4R_WzXmnpI";
const requestBody = {
    month: "feb",
    year: "2025"
};

// Create an AbortController for timeout
const controller = new AbortController();
const timeout = 1200000; // 20 minutes timeout

// Set a timeout to abort the request
const timeoutId = setTimeout(() => controller.abort(), timeout);


const fetchAndStoreStream = async () => {
  try {
    const database = await connectDB();
    const collection = database.collection("reportdata");

    const response = await fetch(url, {
        method: "POST",
        headers: {
            "Authorization": `Bearer ${token}`,
            "Content-Type": "application/json"
        },
        body: JSON.stringify(requestBody),
        signal: controller.signal, // Pass the AbortController signal
    });

    // Clear the timeout if the request completes in time
    clearTimeout(timeoutId);

    if (!response.ok) {
        throw new Error(`HTTP error! Status: ${response.status}`);
    }

    const reader = response.body.getReader();
    let decoder = new TextDecoder();

    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        console.log("streaming done")
        break;
    };

      let jsonString = decoder.decode(value, { stream: true });
      let dataArray = JSON.parse(jsonString);

      // Insert data into MongoDB
      if (Array.isArray(dataArray) && dataArray.length > 0) {
        console.log("data inserted");
        
        await collection.insertMany(dataArray);
      }
    }

    console.log("Data streaming and insertion completed.");
  } catch (error) {
    console.error("Error processing stream:", error);
  } finally {
    // await client.close();
  }
};

fetchAndStoreStream();

// module.exports  = fetchAndStoreStream;
