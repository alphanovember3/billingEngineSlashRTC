const User = require('../model/billingModel');

// Controller methods
const getAllClients = async (req, res) => {
  try {
    const users = await User.getCLientDetails();
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

module.exports = {getAllClients, getInvoice}