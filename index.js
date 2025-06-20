const fs = require('fs');
const readline = require('readline');
const { parseString } = require('fast-csv');
const { Kafka } = require('kafkajs');
require('dotenv').config();

const CONFIG = {
  csvFilePath: process.env.CSV_FILE_PATH,
  offsetFilePath: process.env.OFFSET_FILE_PATH,
  batchSize: parseInt(process.env.BATCH_SIZE) || 10000,
  pollInterval: process.env.POLL_INTERVAL_MS,
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
    topic: process.env.KAFKA_TOPIC || 'json-high-throughput',
  },
};

const HEADER_MAPPING = {
    'Membr id': 'membr_id',
    'trdr id': 'trdr_id',
    'scrp code': 'scrp_code',
    'scrp id': 'scrp_id',
    'rate': 'rate',
    'qty': 'qty',
    'trd status': 'trd_status',
    'Cm code': 'cm_code',
    'Time': 'time',
    'Date': 'date',
    'Clnt id': 'clnt_id',
    'Ordr id': 'ordr_id',
    'Trns typ/Ordr typ': 'trns_type',
    'B/S': 'bs_flag',
    'Trade ID': 'trade_id',
    'Clnt typ': 'clnt_type',
    'ISIN': 'isin',
    'scrp group': 'scrp_group',
    'Sett No': 'sett_no',
    'Ord Time': 'ord_time',
    'Ao/Po flag': 'ao_po_flag',
    'Location id': 'location_id',
    'Trd modi. time/time': 'trd_mod_time',
    'Sessn Id or trdr Id': 'session_id',
    'CP Code': 'cp_code',
    'CP code Confrn': 'cp_code_confrn',
    'Old Cust Prtcpnt': 'old_cust_participant',
    'Old Cust code': 'old_cust_code'
};

const CSV_HEADERS = Object.keys(HEADER_MAPPING);
const MAPPED_HEADERS = Object.values(HEADER_MAPPING);
const OFFSET_FILE = CONFIG.offsetFilePath;
let lastProcessedSize = loadOffset();
const FILE_PATH = CONFIG.csvFilePath;
const POLL_INTERVAL_MS = CONFIG.pollInterval;
const BATCH_SIZE = CONFIG.batchSize;
let batch = [];

const FIELD_TYPES = {
    'Membr id': 'int',
    'trdr id': 'int',
    'scrp code': 'int',
    'scrp id': 'str',
    'rate': 'float',
    'qty': 'int',
    'trd status': 'int',
    'Cm code': 'int',
    'Time': 'str',
    'Date': 'str',
    'Clnt id': 'str',
    'Ordr id': 'BigInt', 
    'Trns typ/Ordr typ': 'str',
    'B/S': 'str',
    'Trade ID': 'int',
    'Clnt typ': 'str',
    'ISIN': 'str',
    'scrp group': 'str',
    'Sett No': 'str',
    'Ord Time': 'str',
    'Ao/Po flag': 'bool',
    'Location id': 'BigInt', 
    'Trd modi. time/time': 'str',
    'Sessn Id or trdr Id': 'int',
    'CP Code': 'str',
    'CP code Confrn': 'str',
    'Old Cust Prtcpnt': 'str',
    'Old Cust code': 'str',
};

// Kafka producer setup
const kafka = new Kafka({ brokers: CONFIG.kafka.brokers });
const producer = kafka.producer();

async function initKafka() {
  await producer.connect();
  console.log('Kafka Producer connected.');
}

async function saveOffset(offset) {
  fs.writeFileSync(OFFSET_FILE, JSON.stringify({ offset }));
}

function loadOffset() {
  if (!fs.existsSync(OFFSET_FILE)) return 0;
  const data = fs.readFileSync(OFFSET_FILE);
  return JSON.parse(data).offset || 0;
    // return 0;   
}

// Utility to parse scientific notation into BigInt safely
function parseScientificBigInt(str) {
    if (!str) return '';
    str = str.toString().trim();
    if (!str.includes('e') && !str.includes('E')) {
        return BigInt(str);
    }
    const [coeff, exp] = str.toLowerCase().split('e');
    const multiplier = Math.pow(10, Number(exp));
    const result = BigInt(Math.round(parseFloat(coeff) * multiplier));
    return result;
}

function parseValue(value, type) {
    if (value === undefined || value === null || value.toString().trim() === '') {
        return ''; // handle empty cells
    }

    value = value.toString();

    try {
        switch(type) {
            case 'int':
                return parseInt(value, 10);
            case 'float':
                return parseFloat(value);
            case 'bool':
                 (value.toLowerCase() === 'true' || value === '1' ? 1 : 0);
            case 'BigInt':
                return parseScientificBigInt(value).toString();
            case 'str':
            default:
                return value;
        }
    } catch (err) {
        console.error(`Failed to parse value "${value}" as ${type}:`, err);
        return null;
    }
}

// This will parse one CSV row object:
function parseRow(csvRow) {
    const parsedRow = {};
    for (const [header, type] of Object.entries(FIELD_TYPES)) {
        parsedRow[header] = parseValue(csvRow[header], type);
    }
    return parsedRow;
}

async function parseCsvRow(line) {
  const values = line.split(',');
  if (values.length !== CSV_HEADERS.length) {
    throw new Error('CSV row column count mismatch');
  }
  const data = {};
  for (let i = 0; i < CSV_HEADERS.length; i++) {
    data[CSV_HEADERS[i]] = values[i].trim();
  }
  return data;
}


function mapRow(row) {
  const mapped = {};
//   console.log(row);
  for (const [csvHeader, dbField] of Object.entries(HEADER_MAPPING)) {
    let value = row[csvHeader];
    mapped[dbField] = value === undefined || value === '' ? '' : value;
  }
  return mapped;
}

async function insertBatch() {
  if (batch.length === 0) return;

  try {
    const messages = batch.map(row => ({
      value: JSON.stringify(row),
    }));

    await producer.send({
      topic: CONFIG.kafka.topic,
      messages: messages,
    });

    console.log(`Published ${batch.length} messages to Kafka`);
  } catch (err) {
    console.error('Kafka publish error:', err);
  }
  batch = [];
}

async function processNewData() {
  const stats = fs.statSync(FILE_PATH);
  if (stats.size === lastProcessedSize) return;

  const stream = fs.createReadStream(FILE_PATH, { start: lastProcessedSize });
  const rl = readline.createInterface({ input: stream });

  let firstLine = lastProcessedSize === 0;

  for await (const line of rl) {
    if (firstLine) {
      firstLine = false;
      continue;  // skip header row only on first read
    }
    try {
      const parsedRow = await parseCsvRow(line);
    //   console.log(parsedRow);
      const typeParsed = parseRow(parsedRow);
      const mapped = mapRow(typeParsed);
      batch.push(mapped);

      if (batch.length >= BATCH_SIZE) await insertBatch();
    } catch (err) {
      console.error('Parsing error:', err);
    }
  }

  lastProcessedSize = stats.size;
  await saveOffset(lastProcessedSize);
  await insertBatch();
}

function startPolling() {
  setInterval(() => {
    processNewData();
  }, POLL_INTERVAL_MS);
}

// Initialize Kafka then start polling
(async () => {
  await initKafka();
  startPolling();
})();
