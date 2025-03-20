const express = require('express');
const dotenv = require('dotenv');
const cors = require('cors'); 
dotenv.config();

const app = express();
app.use(express.json()); 
app.use(cors()); 

const apiRoutes = require('../routes/router');
app.use('/api', apiRoutes);

module.exports = app;