// import express from "express"
const express =require('express')
// import connectDB from './common/mongo.js'
const connectDB = require('./common/mongo.js') 
// import dotenv from 'dotenv'
// dotenv.config()
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3000;
// import funct from './controller/mongoController.js'
const funct = require('./controller/mongoController')
app.use(express.json());

const startServer = async () => {
  try {
    await connectDB(); 
    console.log("MongoDB connected successfully");

    funct(); 

    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1); 
   }
};

startServer();
