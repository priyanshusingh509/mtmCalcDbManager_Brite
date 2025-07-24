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
    topic: "csv_ingest_bse",
  },
};

// Maps CSV column headers to database field names
const HEADER_MAPPING = {
  "Membr id": "membr_id",
  "trdr id": "trdr_id",
  "scrp code": "scrp_code",
  "scrp id": "scrp_id",
  rate: "rate",
  qty: "qty",
  "trd status": "trd_status",
  "Cm code": "cm_code",
  Time: "time",
  Date: "date",
  "Clnt id": "clnt_id",
  "Ordr id": "ordr_id",
  "Trns typ/Ordr typ": "trns_type",
  "B/S": "bs_flag",
  "Trade ID": "trade_id",
  "Clnt typ": "clnt_type",
  ISIN: "isin",
  "scrp group": "scrp_group",
  "Sett No": "sett_no",
  "Ord Time": "ord_time",
  "Ao/Po flag": "ao_po_flag",
  "Location id": "location_id",
  "Trd modi. time/time": "trd_mod_time",
  "Sessn Id or trdr Id": "session_id",
  "CP Code": "cp_code",
  "CP code Confrn": "cp_code_confrn",
  "Old Cust Prtcpnt": "old_cust_participant",
  "Old Cust code": "old_cust_code",
  "hardcoded_1": "hardcoded_1",
  "hardcoded_2": "hardcoded_2",
  "hardcoded_3": "hardcoded_3",
  "hardcoded_4": "hardcoded_4",
};

// Defines the data types for each CSV field
const FIELD_TYPES = {
  "Membr id": "int",
  "trdr id": "int",
  "scrp code": "int",
  "scrp id": "str",
  rate: "float",
  qty: "int",
  "trd status": "int",
  "Cm code": "int",
  Time: "str",
  Date: "str",
  "Clnt id": "str",
  "Ordr id": "BigInt",
  "Trns typ/Ordr typ": "str",
  "B/S": "str",
  "Trade ID": "int",
  "Clnt typ": "str",
  ISIN: "str",
  "scrp group": "str",
  "Sett No": "str",
  "Ord Time": "str",
  "Ao/Po flag": "bool",
  "Location id": "BigInt",
  "Trd modi. time/time": "str",
  "Sessn Id or trdr Id": "int",
  "CP Code": "str",
  "CP code Confrn": "str",
  "Old Cust Prtcpnt": "str",
  "Old Cust code": "str",
  "hardcoded_1": "str",
  "hardcoded_2": "str",
  "hardcoded_3": "str",
  "hardcoded_4": "str",
};


const CSV_HEADERS = Object.keys(HEADER_MAPPING);

// Global variables for file handling and processing state
let handle = null;         // File handle for the current CSV file
let offset = 0;            // Current read position in the file
let currentFilePath = "";  // Path to the currently processed file
let partialLine = "";      // Buffer for incomplete lines from chunked reads
let batch = [];            // Accumulator for records before publishing to Kafka

// Initialize Kafka client and producer
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

/**
 * Parse scientific notation numbers into BigInt
 * @param {string} str - Number in scientific notation (e.g., '1e6')
 * @returns {string} - String representation of the BigInt
 */
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

/**
 * Parse a string value according to the specified type
 * @param {string} value - The value to parse
 * @param {string} type - Target data type ('int', 'float', 'bool', 'BigInt', 'str')
 * @returns {any} - Parsed value or null if parsing fails
 */
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

/**
 * Map CSV row to database record format
 * @param {Object} row - Raw CSV row object
 * @returns {Object} - Mapped record with proper types and UUID
 */
function mapRow(row) {
  const mapped = {};
  for (const [csvHeader, dbField] of Object.entries(HEADER_MAPPING)) {
    const type = FIELD_TYPES[csvHeader] || "str";
    mapped[dbField] = parseValue(row[csvHeader], type);
  }
  mapped["_uuid"] = randomUUID();
  return mapped;
}

/**
 * Parse a single CSV line into an object
 * @param {string} line - CSV formatted string
 * @returns {Promise<Object>} - Parsed CSV row
 */
function parseCsvRow(line) {
  return new Promise((resolve, reject) => {
    parseString(line, { headers: CSV_HEADERS, delimiter: "|", strictColumnHandling: true })
      .on("error", reject)
      .on("data", resolve);
  });
}

/**
 * Publish accumulated records to Kafka topic
 * Uses GZIP compression for efficient data transfer
 */
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

/**
 * Load file read offset from disk
 * @returns {Promise<number>} - Last read position in the file
 */
async function loadOffset() {
  const offsetFile = `${CONFIG.offsetFilePath}/${currentFilePath.split("/").pop()}.offset`;
  try {
    const data = await _readFile(offsetFile, "utf-8");
    return JSON.parse(data).offset || 0;
  } catch (err) {
    return 0;
  }
}

/**
 * Save current read offset to disk
 * @param {number} offset - Current read position to save
 */
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
      const remaining = stat.size - position;
      const readSize = Math.min(CONFIG.chunkSize, remaining);
      const buffer = Buffer.alloc(readSize);
      const { bytesRead } = await fileHandle.read(buffer, 0, readSize, position);
      if (bytesRead === 0) break;
      const data = partialLine + buffer.slice(0, bytesRead).toString();
      const lines = data.split("\n");
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
  const file = `/csvData/EQ_ITR_6758_${format(today, "yyyyMMdd")}.csv`;

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

(async () => {
  await initKafka();
  await switchToNewFile();
  scheduleDailyFileSwitch();
  startPolling();
})();
