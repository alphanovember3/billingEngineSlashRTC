const express = require('express');
const router = express.Router();
const controller = require('../controller/billingController');
const authenticateToken = require('../helper/auth')

router.get('/users',  controller.getAllClients);
router.put('/update', controller.updateClient);
router.post('/find', controller.getInvoice);
router.post("/create", controller.createClient);
router.delete("/delete/:businessId", controller.deleteClient);

module.exports = router;