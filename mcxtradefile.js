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
    topic: "mcx_tradefile",
  },
};

const HEADER_MAPPING = {
  "RelatedSecurityId": "related_security_id",
  "Price": "price",
  "LastPrice": "last_price",
  "SideLastPrice": "side_last_price",
  "ClearingTradePrice": "clearing_trade_price",
  "TransactTime": "transact_time",
  "OrderId": "order_id",
  "TerminalInfo": "terminal_info",
  "ClOrderId": "cl_order_id",
  "LastUpdateTime": "last_update_time",
  "StrategyId": "strategy_id",
  "StrategySequenceNo": "strategy_sequence_no",
  "LastQty": "last_qty",
  "SideLastQty": "side_last_qty",
  "CumQuantity": "cum_quantity",
  "LeaveQuantity": "leave_quantity",
  "SimpleSecurityId": "simple_security_id",
  "_echo": "echo",
  "TradeId": "trade_id",
  "OriginalTradeId": "original_trade_id",
  "RootPartyIdExecutingUnit": "root_party_id_executing_unit",
  "RootPartyIdSessionId": "root_party_id_session_id",
  "RootPartyIdExecutingTrader": "root_party_id_executing_trader",
  "RootPartyIdClearingUnit": "root_party_id_clearing_unit",
  "MarketSegmentId": "market_segment_id",
  "SideTradeId": "side_trade_id",
  "MatchDate": "match_date",
  "TradeMatchId": "trade_match_id",
  "StrategyLinkId": "strategy_link_id",
  "TotalNumTradeReports": "total_num_trade_reports",
  "AccountId": "account_id",
  "MultiLegReportType": "multi_leg_report_type",
  "TradeReportType": "trade_report_type",
  "TransferReason": "transfer_reason",
  "RootPartyIdBeneficiary": "root_party_id_beneficiary",
  "RootPartyIdTakeUpTradingFirm": "root_party_id_take_up_trading_firm",
  "RootPartyIdOrderOriginationFirm": "root_party_id_order_origination_firm",
  "AccountType": "account_type",
  "MatchType": "match_type",
  "MatchSubType": "match_sub_type",
  "Side": "side",
  "AggressorIndicator": "aggressor_indicator",
  "TradingCapacity": "trading_capacity",
  "PostionEffect": "position_effect",
  "CustomerOrderHandlingInst": "customer_order_handling_inst",
  "CPCode": "cp_code",
  "OrderCategory": "order_category",
  "OrderType": "order_type",
  "RelatedComplexProduct": "related_complex_product",
  "OrderSide": "order_side",
  "RootPartyClearingOrganization": "root_party_clearing_organization",
  "RootPartyExecutingFirm": "root_party_executing_firm",
  "RootPartyExecutingTrade": "root_party_executing_trade",
  "RootPartyClearingFirm": "root_party_clearing_firm"
};

const FIELD_TYPES = {
  "RelatedSecurityId": "BigInt",
  "Price": "BigInt",
  "LastPrice": "BigInt",
  "SideLastPrice": "BigInt",
  "ClearingTradePrice": "BigInt",
  "TransactTime": "BigInt",
  "OrderId": "BigInt",
  "TerminalInfo": "BigInt",
  "ClOrderId": "BigInt",
  "LastUpdateTime": "BigInt",
  "StrategyId": "int",
  "StrategySequenceNo": "int",
  "LastQty": "int",
  "SideLastQty": "BigInt",
  "CumQuantity": "int",
  "LeaveQuantity": "int",
  "SimpleSecurityId": "int",
  "_echo": "int",
  "TradeId": "int",
  "OriginalTradeId": "int",
  "RootPartyIdExecutingUnit": "int",
  "RootPartyIdSessionId": "int",
  "RootPartyIdExecutingTrader": "int",
  "RootPartyIdClearingUnit": "int",
  "MarketSegmentId": "int",
  "SideTradeId": "int",
  "MatchDate": "int",
  "TradeMatchId": "int",
  "StrategyLinkId": "int",
  "TotalNumTradeReports": "int",
  "AccountId": "int",
  "MultiLegReportType": "int",
  "TradeReportType": "int",
  "TransferReason": "int",
  "RootPartyIdBeneficiary": "str",
  "RootPartyIdTakeUpTradingFirm": "str",
  "RootPartyIdOrderOriginationFirm": "str",
  "AccountType": "int",
  "MatchType": "int",
  "MatchSubType": "int",
  "Side": "int",
  "AggressorIndicator": "int",
  "TradingCapacity": "int",
  "PostionEffect": "int",
  "CustomerOrderHandlingInst": "int",
  "CPCode": "str",
  "OrderCategory": "int",
  "OrderType": "int",
  "RelatedComplexProduct": "int",
  "OrderSide": "int",
  "RootPartyClearingOrganization": "str",
  "RootPartyExecutingFirm": "int",
  "RootPartyExecutingTrade": "int",
  "RootPartyClearingFirm": "int"
};


const CSV_HEADERS = Object.keys(HEADER_MAPPING);

let handle = null;
let offset = 0;
let currentFilePath = "";
let partialLine = "";
let batch = [];

const kafka = new Kafka({ brokers: CONFIG.kafka.brokers });
const producer = kafka.producer();

function cleanCsvLine(line) {
  return line.trim().replace(/,+$/, ""); // removes all trailing commas
}


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
      .on("error", (error)=>{
        console.log(error);
        reject;
      })
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
      if (bytesRead === 0) break;
    //   console.log("here");
      const data = partialLine + buffer.slice(0, bytesRead).toString();
      const lines = data.split("\n");
    //   console.log(lines[0]);
      partialLine = lines.pop();

      for (let line of lines) {
        try {
          const cleanedLine = cleanCsvLine(line);
          const parsed = await parseCsvRow(cleanedLine);
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
  nextSwitch.setHours(9, 30, 0, 0);
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
  const file = `/4.208_data/mcxtradeDownload_${format(today, "yyyyMMdd")}.csv`;

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
