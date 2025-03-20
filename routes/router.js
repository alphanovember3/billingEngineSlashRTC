const express = require('express');
const router = express.Router();

const controller = require('../controller/billingController');


router.get('/users', controller.getAllClients);
router.put('/update', controller.updateClient);

module.exports = router;