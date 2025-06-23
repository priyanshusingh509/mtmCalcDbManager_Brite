import {
  readFile as _readFile,
  writeFile,
  stat as _stat,
  open as _open,
} from "fs/promises";
import { randomUUID } from "crypto";
import { Kafka } from "kafkajs";
import { parseString } from "fast-csv";
import dotenv from "dotenv";
dotenv.config(); 

const CONFIG = {
  csvFilePath: process.env.CSV_FILE_PATH || "./data.csv",
  offsetFilePath: process.env.OFFSET_FILE_PATH || "./data.offset",
  pollIntervalMs: parseInt(process.env.POLL_INTERVAL_MS) || 100,
  chunkSize: parseInt(process.env.CHUNK_SIZE) || 1024 * 64, // 64KB chunk size
  batchSize: parseInt(process.env.BATCH_SIZE) || 1000,
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
    topic: process.env.KAFKA_TOPIC || "json-high-throughput",
  },
};

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
let headerParsed = false;
let batch = [];

const kafka = new Kafka({ brokers: CONFIG.kafka.brokers });
const producer = kafka.producer();

async function initKafka() {
  await producer.connect();
  console.log("Kafka connected.");
}

async function loadOffset() {
  try {
    const data = await _readFile(CONFIG.offsetFilePath, "utf-8");
    return JSON.parse(data).offset || 0;
  } catch (err) {
    return 0; // default if file doesn't exist yet
  }
}

async function saveOffset(offset) {
  await writeFile(CONFIG.offsetFilePath, JSON.stringify({ offset }));
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
    parseString(line, { headers: CSV_HEADERS, delimiter: "|", strictColumnHandling: true })
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
    });
    console.log(`Published ${batch.length} records`);
  } catch (err) {
    console.error("Kafka Error:", err);
  }
  batch = [];
}

let partialLine = "";
let offset = 0;

async function readFile() {
  const stat = await _stat(CONFIG.csvFilePath);
  if (stat.size === offset) {
    return;
  }

  let handle;
  try {
    handle = await _open(CONFIG.csvFilePath, "r");
    let position = offset;

    while (position < stat.size) {
      const remaining = stat.size - position;
      const readSize = Math.min(CONFIG.chunkSize, remaining);
      const buffer = Buffer.alloc(readSize);
      const { bytesRead } = await handle.read(buffer, 0, readSize, position);
      if (bytesRead === 0) break;

      const data = partialLine + buffer.slice(0, bytesRead).toString();
      const lines = data.split("\n");
      partialLine = lines.pop();

      for (let line of lines) {
        if (!headerParsed && line.startsWith("Membr id")) {
          headerParsed = true;
          continue;
        }
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
    saveOffset(offset);
  } catch (err) {
    console.error("File read error:", err);
  } finally {
    if (handle) {
      await handle.close();
    }
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

(async () => {
  await initKafka();
  offset = await loadOffset();
  startPolling();
})();
