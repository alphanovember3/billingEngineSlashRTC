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

const createClient = async(req)=>{
  try {
    let {businessId, clientApi, cronDate, token, clientName ,didInfoApi, licenceApi}=req.body;
    const db = await connectDB();
    const collection = await db.collection('clientDetails');
    const response = await collection.insertOne({
          businessId:businessId,
          clientApi:clientApi,
          cronDate:cronDate,
          token:token,
          clientName:clientName,
          didInfoApi:didInfoApi,
          licenceApi:licenceApi
        });
    return response;
  } catch (error) {
    console.log("error occurred in creating client details: ", error);
    return error;
  }
}
module.exports = {getClientDetails, getInvoice, getUpdateDetails, createClient}