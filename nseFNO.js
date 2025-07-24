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
    topic: "nse_fno_algo",
  },
};

const HEADER_MAPPING = {
  "ID": "id",
  "Trade Number": "trade_number",
  "Trade Time": "trade_time",
  "Buy/Sell": "buy_sell",
  "Symbol": "symbol",
  "Instrument Type": "inst_type",
  "Option Type": "opt_type",
  "Expiry": "expiry",
  "Strike Price": "strike_price",
  "Trade Quantity": "trade_qty",
  "Trade Price": "trade_price",
  "CTCL ID": "ctcl_id",
  "Broker ID": "broker_id",
  "Strategy ID": "strategy_id",
  "Strategy Name": "strategy_name",
  "User ID": "user_id",
  "Token": "token",
  "Exchange": "exchange",
  "Segment": "segment",
  "Response Order Number": "response_order_number",
  "Settlor": "settlor",
  "Old Settlor": "old_settlor",
  "Account Number": "account_number",
  "Old Account Number": "old_account_number",
  "Original Volume": "original_vol",
  "Disclosed Volume": "disclosed_vol",
  "Remaining Volume": "remaining_vol",
  "Disclosed Volume Remaining": "disclosed_vol_remaining",
  "Order Price": "order_price",
  "GTD": "gtd",
  "Volume Filled Today": "vol_filled_today",
  "Activity Type": "activity_type",
  "OP Order Number": "op_order_number",
  "OP Broker ID": "op_broker_id",
  "Open/Close": "open_close",
  "Old Open/Close": "old_open_close",
  "Book Type": "book_type",
  "New Volume": "new_volume",
  "Give Up": "give_up",
  "PAN": "pan",
  "Old PAN": "old_pan",
  "Algo ID": "algo_id",
  "Algo Category": "algo_category",
  "Last Activity Reference": "last_activity_reference",
  "NNF": "nnf"
};

const FIELD_TYPES = {
  "ID": "int",
  "Trade Number": "int",
  "Trade Time": "int",
  "Buy/Sell": "str",
  "Symbol": "str",
  "Instrument Type": "str",
  "Option Type": "str",
  "Expiry": "int",
  "Strike Price": "int",
  "Trade Quantity": "int",
  "Trade Price": "int",
  "CTCL ID": "int",
  "Broker ID": "int",
  "Strategy ID": "int",
  "Strategy Name": "int",
  "User ID": "int",
  "Token": "int",
  "Exchange": "str",
  "Segment": "str",
  "Response Order Number": "BigInt",
  "Settlor": "str",
  "Old Settlor": "str",
  "Account Number": "str",
  "Old Account Number": "str",
  "Original Volume": "int",
  "Disclosed Volume": "int",
  "Remaining Volume": "int",
  "Disclosed Volume Remaining": "int",
  "Order Price": "int",
  "GTD": "int",
  "Volume Filled Today": "int",
  "Activity Type": "str",
  "OP Order Number": "BigInt",
  "OP Broker ID": "int",
  "Open/Close": "int",
  "Old Open/Close": "int",
  "Book Type": "int",
  "New Volume": "int",
  "Give Up": "int",
  "PAN": "str",
  "Old PAN": "str",
  "Algo ID": "int",
  "Algo Category": "int",
  "Last Activity Reference": "BigInt",
  "NNF": "BigInt"
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
      // console.log(bytesRead);
      if (bytesRead === 0) break;
    //   console.log("here");
      const data = partialLine + buffer.slice(0, bytesRead).toString();
      const lines = data.split("\n");
      // console.log(lines[0]);
      partialLine = lines.pop();

      for (let line of lines) {
        try {
          // console.log(line)
          const parsed = await parseCsvRow(line);
          // console.log(parsed)
          const mapped = mapRow(parsed);
          // console.log(mapped)
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
    // console.log(batch)
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
  const file = `/4.208_data/check_share/DC/trades_info.${format(today, "yyyy-MM-dd")}.csv`;

  while (true) {
    try {
      await openCsvFile(file);
      break; // success — exit the loop
    } catch (err) {
      console.error("Failed to open new CSV file:", err);
      console.log("Retrying in 30 seconds...");
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
