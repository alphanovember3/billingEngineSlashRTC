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
    console.log("Request Body:", req.body);

    const createdClient = await User.getCreateClients(req);
    if (!createdClient) {
      return res.status(400).json({ message: "Client details not inserted" });
    }
    
    res.status(201).json({ message: "Client created successfully", data: createdClient });
  } catch (error) {
    console.error("Error creating client:", error);
    res.status(500).json({ message: "Internal server error", error: error.message });
  }
};

const deleteClient = async (req, res) => {
  try {
    console.log("Request Body:", req.body);

    const { businessId } = req.body;

    if (!businessId) {
      return res.status(400).json({ message: "businessId is required" });
    }

    const deletedClient = await User.getDeleteClient(businessId);

    if (!deletedClient.deletedCount) {
      return res.status(404).json({ message: "Client not found" });
    }

    res.status(200).json({ message: "Client deleted successfully" });
  } catch (error) {
    console.error("Error deleting client:", error);
    res.status(500).json({ message: "Internal server error", error: error.message });
  }
};

module.exports = {getAllClients, getInvoice, updateClient, createClient, deleteClient}
