import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand
} from "@aws-sdk/client-s3";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";

const lambda = new LambdaClient({ region: "us-east-1" });
const s3 = new S3Client({ region: "us-east-1" });
const bucketName = process.env.BUCKET_NAME;

const LogTickerList = ['AMZN','SPY', 'QQQ', 'IWM','AAPL', 'NVDA', 'GOOG', 'TSLA','SOXL', 'SOFI', 'AAL','INTC', 'TSLL','AAPU'];
const ETF_List = ['SPY', 'QQQ', 'IWM', 'TQQQ', 'SOXL', 'TSLL', 'SQQQQ','AAPU'];


export const handler = async (event) => {

  let results = [];
  for (const ticker of LogTickerList) {
    console.log('inside handler now processing ticker-', ticker);
    await startWritingProcess(ticker);
  }

  return {
    statusCode: 200,
    headers: {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "*",
    },
    body: JSON.stringify(results),
  };
};

const startWritingProcess = async (selectedTicker) => {
  try {
    let selectedDate = '';
    let selectedDayOrMonth = 'day';
    const assetclass = ETF_List.includes(selectedTicker) ? 'ETF' : 'stocks';

    if (selectedDayOrMonth === 'day' && assetclass === 'ETF') {
      selectedDate = ["TQQQ", "SOXL", "TSLL", "SQQQ"].includes(selectedTicker)
        ? getComingFriday()
        : getEffectiveDate()
    } else if (selectedDayOrMonth === 'day' && assetclass === 'stocks') {
      selectedDate = getComingFriday()
    }

    const payload = {
      selectedTicker: selectedTicker,
      assetclass: assetclass,
      selectedDayOrMonth: selectedDayOrMonth,
      inputDate: selectedDate,
    };

    console.log('**** Invoking mywelcomefunction with payload:', payload);

    const command = new InvokeCommand({
      FunctionName: "mywelcomefunction",
      Payload: Buffer.from(JSON.stringify({ queryStringParameters: payload })),
    });

    const response = await lambda.send(command);

    const raw = Buffer.from(response.Payload).toString();
    // console.log('🧪 Raw response:', raw);

    let parsed = JSON.parse(raw); // First parse

    // ✅ If it's a Lambda proxy-style response
    if (parsed.body) {
      console.log('🧪 Detected API Gateway style response, parsing body...');
      parsed = JSON.parse(parsed.body); // Second parse
    }

    const latestData = parsed;
    // console.log('📦 latestData:', JSON.stringify(latestData, null, 2));

    let lstPrice = latestData?.data?.lastTrade || '';
    const match = lstPrice ? lstPrice.match(/\$([\d.]+)/) : null;
    lstPrice = match ? parseFloat(match[1]) : 0;

    const rows = latestData?.data?.table?.rows || [];

    // Extract numeric value if it's like "$217.12"
    // const match = lstPrice ? lstPrice.match(/\$([\d.]+)/) : null;
    // lstPrice = match ? parseFloat(match[1]) : 0;

    // const rows = latestData?.data?.table?.rows || [];
    // console.log('Parsed table rows:', rows);

    const total = await caculateSum(rows);
    //console.log(' caculateSum ..call volume is---.', total.c_Volume);

    const idTemp = Date.now().toString(36) + Math.random().toString(36).substring(2);
    const newEntry = {
      id: idTemp,
      timestamp: getTodayInEST(false),
      callVolume: total.c_Volume,
      putVolume: total.p_Volume,
      selectedTicker: selectedTicker,
      lstPrice: lstPrice
    };
    await appendToS3JsonArray(newEntry);
    return { ticker: selectedTicker, status: 'success' };

  } catch (err) {
    console.error(`Error with ${selectedTicker}:`, err);
    return { ticker: selectedTicker, status: 'failed', error: err.message };
  }
}
async function caculateSum(rows) {
  let c_Volume = 0;
  let p_Volume = 0;

  for (const row of rows) {
    const cRaw = row.c_Volume;
    const pRaw = row.p_Volume;
    const cVol = parseInt(cRaw?.replace(/,/g, '')) || 0;
    const pVol = parseInt(pRaw?.replace(/,/g, '')) || 0;
    c_Volume += cVol;
    p_Volume += pVol;
  }

  return { c_Volume, p_Volume };
}


const getTodayInEST = (isFileName) => {
  const estDate = new Date().toLocaleString("en-US", {
    timeZone: "America/New_York"
  });
  const date = new Date(estDate);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");
  return isFileName ? `${year}-${month}-${day}` : `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
};

const streamToString = async (stream) => {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf-8")));
  });
};

const checkIfFileExists = async (key) => {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucketName, Key: key }));
    return true;
  } catch (err) {
    if (err.name === "NotFound") return false;
    throw err;
  }
};

const createJsonFile = async (key, data) => {
  await s3.send(new PutObjectCommand({
    Bucket: bucketName,
    Key: key,
    Body: JSON.stringify(data, null, 2),
    ContentType: "application/json",
  }));
};

const appendToS3JsonArray = async (newObject) => {
  const FILE_KEY = `${getTodayInEST(true)}.json`;
  let dataArray = [];
  const exists = await checkIfFileExists(FILE_KEY);
  if (!exists) {
    await createJsonFile(FILE_KEY, []);
    console.log('File created successfully');
  }

  try {

    const getCommand = new GetObjectCommand({ Bucket: bucketName, Key: FILE_KEY });
    const response = await s3.send(getCommand);
    const bodyString = await streamToString(response.Body);
    dataArray = JSON.parse(bodyString);

    const newId = (dataArray.at(-1)?.id || 0) + 1;
    const timestamp = new Date().toISOString();

    const newEntry = { id: newId, timestamp, ...newObject };

    dataArray.push(newEntry);

    const putCommand = new PutObjectCommand({
      Bucket: bucketName,
      Key: FILE_KEY,
      Body: JSON.stringify(dataArray, null, 2),
      ContentType: "application/json",
    });
   const resp= await s3.send(putCommand);

    // const payload = {
    //   Bucket: bucketName,
    //   Key: FILE_KEY,
    //   Body: JSON.stringify(dataArray, null, 2),
    //   ContentType: "application/json",
    // };
    // console.log('**** inside trying to send new data to s3 bucket data...');
    // const command = new InvokeCommand({
    //   FunctionName: "sThreeWrite", // replace with actual Lambda name or ARN
    //   Payload: Buffer.from(JSON.stringify({ queryStringParameters: payload })),
    // });

    //   const command = new InvokeCommand({
    //     FunctionName: "sThreeWrite",
    //     Payload: Buffer.from(JSON.stringify({
    //       callVolume: newObject.callVolume,
    //       putVolume: newObject.putVolume,
    //       lstPrice: newObject.lstPrice,
    //       selectedTicker: newObject.selectedTicker
    //     })),
    //   });
    //   console.log('command to send s3 write', command)
    //  const updatedresponse= await lambda.send(command);

    //  console.log(`response of sThreeWrite--: ${JSON.stringify(updatedresponse)}`);
    console.log(`✅ Appended and uploaded JSON file to S3: ${FILE_KEY}`);
    return resp;
  } catch (error) {
    console.log('error in appendToS3JsonArray', error)
  }
};
function getEffectiveDate() {
  const today = new Date();
  const day = today.getDay(); // 0 = Sunday, 6 = Saturday

  if (day === 6) {
    today.setDate(today.getDate() + 2); // Saturday → Monday
  } else if (day === 0) {
    today.setDate(today.getDate() + 1); // Sunday → Monday
  }

  // Format as 'yyyy-mm-dd'
  const yyyy = today.getFullYear();
  const mm = String(today.getMonth() + 1).padStart(2, '0');
  const dd = String(today.getDate()).padStart(2, '0');
  const effectiveDate = `${yyyy}-${mm}-${dd}`
  console.log('--getEffectiveDate--', effectiveDate)
  return effectiveDate;
}

function getComingFriday() {
  const today = new Date();

  const currentDay = today.getDay(); // 0 = Sunday, ..., 6 = Saturday
  const daysUntilFriday = (5 - currentDay + 7) % 7; // 5 = Friday

  const comingFriday = new Date(today);
  comingFriday.setDate(today.getDate() + daysUntilFriday);

  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });

  const [month, day, year] = formatter.format(comingFriday).split('/');
  const comingFridayDate = `${year}-${month}-${day}`;
  console.log('--comingFridayDate--', comingFridayDate)
  return comingFridayDate;
}
