const express = require('express');
const router = express.Router();

const controller = require('../controller/billingController');


router.get('/users', controller.getAllClients);
// router.post('/users', controller.createUser);

module.exports = router;