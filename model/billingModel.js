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
      { "test.businessId": businessId },  
      {
        $set: {
          "test.clientName": clientName,
          "test.clientApi": clientApi,
          "test.cronDate": cronDate,
          "test.token": token
        }
      }
    );
    return response;
  } catch (error) {
    console.log("error occurred in updating client details: ", error);
    return [];
  }
};

const getInvoice=async (bId, month, year)=>{
  try {
      const db= await connectDB();
      const collection =await db.collection('billData')
      const response = await collection.find({}).toArray()
      return response;
    } catch (error) {
      console.log("error occured in fetching client details: ", error)
      return [];
    }
}

module.exports = {getClientDetails, getInvoice, getUpdateDetails}
