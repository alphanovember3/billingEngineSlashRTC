const {connectDB} = require('../common/mongo');

const getCLientDetails=async ()=>{
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

module.exports = {getCLientDetails, getInvoice}
