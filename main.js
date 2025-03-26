const express =require('express')
const {connectDB} = require('./common/mongo.js') 

require("dotenv").config();

const app = require('./api/api.js')
const PORT = process.env.PORT || 3000;

// const funct = require('./controller/mongoController')
app.use(express.json());

const startServer = async () => {
  try {
    await connectDB(); 
    console.log("MongoDB connected successfully");

    // funct(); 

    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1); 
   }
};

startServer();
