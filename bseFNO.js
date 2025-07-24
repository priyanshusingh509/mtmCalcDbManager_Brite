import {
  readFile as _readFile,
  writeFile,
  stat as _stat,
  open as _open,
} from "fs/promises";
import { randomUUID } from "crypto";
import { CompressionTypes, Kafka } from "kafkajs";
import { parseString } from "fast-csv";
import dotenv from "dotenv";
import { format } from "date-fns";

dotenv.config();

const CONFIG = {
  offsetFilePath: "/home/hp/baseServer/offset",
  pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS) || 100,
  chunkSize: parseInt(process.env.CHUNK_SIZE) || 1024 * 64,
  batchSize: parseInt(process.env.BATCH_SIZE) || 1000,
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
    topic: "eqd_itrtm_csv",
  },
};

const HEADER_MAPPING = {
  "Trade Number": "TradeNumber",
  "Trade Date Time": "TradeDateTime",
  "Trade Status": "TradeStatus",
  "Segment Indicator": "SegmentIndicator",
  "Settlement Type": "SettlementType",
  "Product Type": "ProductType",
  "Product Code": "ProductCode",
  "Asset Code": "AssetCode",
  "Expiry Date": "ExpiryDate",
  "Strike Price": "StrikePrice",
  "Option Type": "OptionType",
  "Series Code": "SeriesCode",
  "Buy Broker": "BuyBroker",
  "Sell Broker": "SellBroker",
  "Trade Price": "TradePrice",
  "Trade Quantity": "TradeQuantity",
  "Series ID": "SeriesID",
  "Trade Buyer Location ID": "TradeBuyerLocationID",
  "Buy CM Code": "BuyCMCode",
  "Sell CM Code": "SellCMCode",
  "Trade Seller Location ID": "TradeSellerLocationID",
  "Buy Custodial Participant": "BuyCustodialParticipant",
  "Buy Side Confirmation": "BuySideConfirmation",
  "Sell Custodial Participant": "SellCustodialParticipant",
  "Sell Side Confirmation": "SellSideConfirmation",
  "Buy Covered Uncovered Flag": "BuyCoveredUncoveredFlag",
  "Sell Covered Uncovered Flag": "SellCoveredUncoveredFlag",
  "Buy Old Custodial Participant": "BuyOldCustodialParticipant",
  "Buy Old CM Code": "BuyOldCMCode",
  "Sell Old Custodial Participant": "SellOldCustodialParticipant",
  "Sell Old CM Code": "SellOldCMCode",
  "Trade Buyer Terminal ID": "TradeBuyerTerminalID",
  "Trade Seller Terminal ID": "TradeSellerTerminalID",
  "Buy Order No": "BuyOrderNo",
  "Sell Order No": "SellOrderNo",
  "Buy Client Code": "BuyClientCode",
  "Sell Client Code": "SellClientCode",
  "Buy Remarks": "BuyRemarks",
  "Sell Remarks": "SellRemarks",
  "Buy Position": "BuyPosition",
  "Sell Position": "SellPosition",
  "Buy Proprietor/Client Flag": "BuyProprietorClientFlag",
  "Sell Proprietor/Client Flag": "SellProprietorClientFlag",
  "Buy Order Time Stamp": "BuyOrderTimeStamp",
  "Sell Order Time Stamp": "SellOrderTimeStamp",
  "Buy Order Active Flag": "BuyOrderActiveFlag",
  "Sell Order Active Flag": "SellOrderActiveFlag"
};


const FIELD_TYPES = {
  "Trade Number": "BigInt",
  "Trade Date Time": "str",
  "Trade Status": "int",
  "Segment Indicator": "str",
  "Settlement Type": "str",
  "Product Type": "str",
  "Product Code": "str",
  "Asset Code": "str",
  "Expiry Date": "str",
  "Strike Price": "float",
  "Option Type": "str",
  "Series Code": "str",
  "Buy Broker": "str",
  "Sell Broker": "str",
  "Trade Price": "float",
  "Trade Quantity": "int",
  "Series ID": "BigInt",
  "Trade Buyer Location ID": "BigInt",
  "Buy CM Code": "str",
  "Sell CM Code": "str",
  "Trade Seller Location ID": "BigInt",
  "Buy Custodial Participant": "str",
  "Buy Side Confirmation": "str",
  "Sell Custodial Participant": "str",
  "Sell Side Confirmation": "str",
  "Buy Covered Uncovered Flag": "str",
  "Sell Covered Uncovered Flag": "str",
  "Buy Old Custodial Participant": "str",
  "Buy Old CM Code": "str",
  "Sell Old Custodial Participant": "str",
  "Sell Old CM Code": "str",
  "Trade Buyer Terminal ID": "str",
  "Trade Seller Terminal ID": "str",
  "Buy Order No": "str",
  "Sell Order No": "str",
  "Buy Client Code": "str",
  "Sell Client Code": "str",
  "Buy Remarks": "str",
  "Sell Remarks": "str",
  "Buy Position": "str",
  "Sell Position": "str",
  "Buy Proprietor/Client Flag": "str",
  "Sell Proprietor/Client Flag": "str",
  "Buy Order Time Stamp": "str",
  "Sell Order Time Stamp": "str",
  "Buy Order Active Flag": "str",
  "Sell Order Active Flag": "str"
};


const CSV_HEADERS = Object.keys(HEADER_MAPPING);

let handle = null;
let offset = 0;
let currentFilePath = "";
let partialLine = "";
let batch = [];

const kafka = new Kafka({ brokers: CONFIG.kafka.brokers });
const producer = kafka.producer();

async function initKafka() {
  while (true) {
    try {
      await producer.connect();
      console.log("Kafka connected.");
      break; // Exit loop if successful
    } catch (error) {
      console.error("Kafka connection failed:", error.message || error);
      console.log("Retrying in 1 minute...");
      await new Promise(resolve => setTimeout(resolve, 60 * 1000)); // Wait 1 minute
    }
  }
}

function parseScientificBigInt(str) {
  if (!str) return "";
  str = str.toString().trim();
  if (!str.includes("e") && !str.includes("E")) {
    return BigInt(str);
  }
  const [coeff, exp] = str.toLowerCase().split("e");
  const multiplier = Math.pow(10, Number(exp));
  const result = BigInt(Math.round(parseFloat(coeff) * multiplier));
  return result;
}

function parseValue(value, type) {
  if (!value || value === "") return null;
  try {
    switch (type) {
      case "int":
        return parseInt(value, 10);
      case "float":
        return parseFloat(value);
      case "bool":
        return value.toLowerCase() === "true" || value === "1";
      case "BigInt":
        return parseScientificBigInt(value).toString();
      case "str":
      default:
        return value;
    }
  } catch {
    return null;
  }
}

function mapRow(row) {
  const mapped = {};
  for (const [csvHeader, dbField] of Object.entries(HEADER_MAPPING)) {
    const type = FIELD_TYPES[csvHeader] || "str";
    mapped[dbField] = parseValue(row[csvHeader], type);
  }
  mapped["_uuid"] = randomUUID();
  return mapped;
}

function parseCsvRow(line) {
  return new Promise((resolve, reject) => {
    parseString(line, { headers: CSV_HEADERS, delimiter: ",", strictColumnHandling: true })
      .on("error", reject)
      .on("data", resolve);
  });
}

async function publishBatch() {
  if (batch.length === 0) return;
  try {
    await producer.send({
      topic: CONFIG.kafka.topic,
      messages: batch.map((data) => ({ value: JSON.stringify(data) })),
      compression: CompressionTypes.GZIP
    });
    console.log(`Published ${batch.length} records`);
  } catch (err) {
    console.error("Kafka Error:", err);
  }
  batch = [];
}

async function loadOffset() {
  const offsetFile = `${CONFIG.offsetFilePath}/${currentFilePath.split("/").pop()}.offset`;
  try {
    const data = await _readFile(offsetFile, "utf-8");
    return JSON.parse(data).offset || 0;
  } catch (err) {
    return 0;
  }
}

async function saveOffset(offset) {
  const offsetFile = `${CONFIG.offsetFilePath}/${currentFilePath.split("/").pop()}.offset`;
  await writeFile(offsetFile, JSON.stringify({ offset }));
}

async function openCsvFile(filePath) {
  if (handle) await handle.close();
  handle = await _open(filePath, "r");
  currentFilePath = filePath;
  offset = await loadOffset();
  partialLine = "";
  console.log(`Switched to new CSV file: ${filePath}, loaded offset: ${offset}`);
}

async function readFile() {
  const stat = await _stat(currentFilePath);
  if (stat.size === offset) return;
  try {
    const fileHandle = await _open(currentFilePath, "r");
    let position = offset;
    while (position < stat.size) {
    //   console.log(position);
      const remaining = stat.size - position;
      const readSize = Math.min(CONFIG.chunkSize, remaining);
    //   console.log(readSize);
      const buffer = Buffer.alloc(readSize);
      const { bytesRead } = await fileHandle.read(buffer, 0, readSize, position);
    //   console.log(bytesRead);
      if (bytesRead === 0) break;
    //   console.log("here");
      const data = partialLine + buffer.slice(0, bytesRead).toString();
      const lines = data.split("\n");
    //   console.log(lines[0]);
      partialLine = lines.pop();

      for (let line of lines) {
        try {
          const parsed = await parseCsvRow(line);
          const mapped = mapRow(parsed);
          batch.push(mapped);
          if (batch.length >= CONFIG.batchSize) await publishBatch();
        } catch (err) {
          console.error("Parse error:", err);
        }
      }
    //   console.log(batch[0]);
    //   console.log("Done");
      position += bytesRead;
      offset = position;
    }
    await publishBatch();
    await saveOffset(offset);
    await fileHandle.close();
  } catch (err) {
    console.error("File read error:", err);
  }
}

let isProcessing = false;

function startPolling() {
  setInterval(async () => {
    if (isProcessing) return;
    isProcessing = true;
    try {
      await readFile();
    } catch (err) {
      console.error("Polling error:", err);
    } finally {
      isProcessing = false;
    }
  }, CONFIG.pollIntervalMs);
}

function scheduleDailyFileSwitch() {
  const now = new Date();
  const nextSwitch = new Date();
  nextSwitch.setHours(9, 15, 0, 0);
  if (now >= nextSwitch) nextSwitch.setDate(nextSwitch.getDate() + 1);

  const delay = nextSwitch - now;

  setTimeout(async () => {
    await switchToNewFile();
    scheduleDailyFileSwitch();
  }, delay);

  console.log(`Next file switch scheduled at: ${nextSwitch.toLocaleString()}`);
}

async function switchToNewFile() {
  const today = new Date();
  const file = `/csvData/EQD_ITRTM_6758_${format(today, "yyyyMMdd")}.csv`;

  while (true) {
    try {
      await openCsvFile(file);
      break; // success, exit loop
    } catch (err) {
      console.error("Failed to open new CSV file:", err);
      console.log("Trying again in 30 seconds...");
      await new Promise(resolve => setTimeout(resolve, 30 * 1000));
    }
  }
}

// const cleanup = async () => {
//   console.log("Closing file handle...");
//   if (handle) await handle.close();
//   process.exit(0);
// };

// process.on("SIGINT", cleanup);
// process.on("SIGTERM", cleanup);
// process.on("exit", cleanup);
// process.on("uncaughtException", (err) => {
//   console.error("Unhandled error:", err);
//   cleanup();
// });

(async () => {
  await initKafka();
  await switchToNewFile();
  scheduleDailyFileSwitch();
  startPolling();
})();
