const {connectDB} = require('../common/mongo');

const getClientDetails=async ()=>{
    try {
        const db= await connectDB();
        const collection =await db.collection('clientDetails')
        const response = await collection.find({}).toArray()
        return response;
      } catch (error) {
        console.log("error occured in fetching client details: ", error)
        return [];
      }
}

const getUpdateDetails = async (req) => {
  try {
    const { businessId, clientName, clientApi, cronDate, token } = req.body;
    const db = await connectDB();
    const collection = await db.collection('clientDetails');
    const response = await collection.updateOne(
      { "businessId": businessId },  
      {
        $set: {
          "clientName": clientName,
          "clientApi": clientApi,
          "cronDate": cronDate,
          "token": token
        }
      }
    );
    return response;
  } catch (error) {
    console.log("error occurred in updating client details: ", error);
    return [];
  }
};

const getInvoice = async (bId, month, year) => {
  try {
    const db = await connectDB();
    const collection = db.collection('billData');
    const query ={
      businessID:bId,
      month :month,
      year: Number(year)
      };

    console.log("Query being executed:", query); 

    const response = await collection.find(query).toArray();

    console.log("Response from DB:", response); 

    return response;
  } catch (error) {
    console.error("Error occurred in fetching invoice details:", error);
    return [];
  }
};

const getCreateClients = async (req) => {
  try {
    let { businessId, clientApi, cronDate, token, clientName, didInfoApi, licenceApi } = req.body;
    const db = await connectDB();
    const collection = db.collection("clientDetails");

    // Check if client already exists
    const existingClient = await collection.findOne({ businessId });
    if (existingClient) {
      throw new Error("Client already exists with this businessId");
    }

    const response = await collection.insertOne({
      businessId,
      clientApi,
      cronDate,
      token,
      clientName,
      didInfoApi,
      licenceApi,
      createdAt: new Date(),
    });

    return response;
  } catch (error) {
    console.error("Error creating client:", error);
    throw error;
  }
};

const getDeleteClient = async (businessId) => {
  try {
    const db = await connectDB();
    const collection = db.collection("clientDetails");

    const deleteResult = await collection.deleteOne({ businessId });

    return deleteResult;
  } catch (error) {
    console.error("Error deleting client:", error);
    throw error;
  }
};

module.exports = {getClientDetails, getInvoice, getUpdateDetails, getCreateClients, getDeleteClient}