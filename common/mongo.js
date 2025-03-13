const { MongoClient } = require("mongodb");
// import pkg from 'mongodb';
// const {MongoClient} = pkg;
// import dotenv from 'dotenv'
// dotenv.config()


require("dotenv").config();

const uri = process.env.MONGO_URI;

const options = {
  useNewUrlParser: true,
  useUnifiedTopology: true,
  maxPoolSize: 10, 
  serverSelectionTimeoutMS: 5000,
};


let client;
let db;

async function connectDB() {
  if (!client) {
    try {
      client = new MongoClient(uri, options);
      await client.connect();
      console.log("Connected to MongoDB successfully!");
      db = client.db(process.env.DB_NAME); 
    } catch (error) {
      console.error("MongoDB Connection Error:", error);
      process.exit(1); 
    }
  }
  return db;
}

module.exports = {connectDB}
