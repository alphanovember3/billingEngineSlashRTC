const User = require('../model/billingModel');

// Controller methods
const getAllClients = async (req, res) => {
  try {
    const users = await User.getClientDetails();
    // console.log(users)
    res.status(200).json(users);
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
};

const getInvoice = async (req, res) => {
  const { bId, month, year } = req.body;
  const invoice = await User.getInvoice(bId, month, year);
  console.log("Invoice", invoice);
  try {
    if (invoice.length === 0) {
      return res.status(404).json({ message: "No invoices found for the given criteria." });
    }
    res.status(200).json(invoice);
  } catch (error) {
    console.error("API error:", error);
    res.status(500).json({ message: "Error fetching invoices." });
  }
}


const updateClient = async (req, res) => {
  try {
      console.log('Request Body:', req.body)
      const updatedClient = await User.getUpdateDetails(req);
      if (!updatedClient) {
          return res.status(404).json({ message: "Client not found" });
      }
      res.status(200).json({ message: "Client updated successfully", data: updatedClient });
  } catch (error) {
      res.status(500).json({ message: error.message });
  }
};


const createClient = async (req, res) => {
  try {
      console.log('Request Body:', req)
      
      const createdClient = await User.createClient(req);
      if (!createdClient) {
          return res.status(404).json({ message: "Client details not inserted" });
      }
      res.status(200).json({ message: "Client created successfully", data: updatedClient });
  } catch (error) {
      res.status(500).json({ message: error.message });
  }
};

module.exports = {getAllClients, getInvoice, updateClient, createClient}
