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
  const newUser = User.getInvoice(bId, month, year);
  try {
    await newUser.save();
    res.status(201).json(newUser);
  } catch (error) {
    res.status(400).json({ message: error.message });
  }
}


const updateClient = async (req, res) => {
  try {
      console.log('Request Body:', req.body); // Log incoming request body
      const updatedClient = await User.getUpdateDetails(req);
      if (!updatedClient) {
          return res.status(404).json({ message: "Client not found" });
      }

      res.status(200).json({ message: "Client updated successfully", data: updatedClient });
  } catch (error) {
      res.status(500).json({ message: error.message });
  }
};


module.exports = {getAllClients, getInvoice, updateClient}