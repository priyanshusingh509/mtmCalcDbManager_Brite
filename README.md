# Market Data Processor

A Node.js application that processes market data from various exchanges (BSE, NSE, MCX) and publishes it to Kafka topics. The application is designed to handle different file formats and data schemas for each exchange and market segment.

## Components

The project consists of the following main components:

1. **bseCash.js** - Processes BSE Cash market data
2. **bseFNO.js** - Processes BSE F&O (Futures and Options) market data
3. **nseCash.js** - Processes NSE Cash market data
4. **nseFNO.js** - Processes NSE F&O market data
5. **mcxtradefile.js** - Processes MCX (Multi Commodity Exchange) trade data

## Features

- **File Processing**: Efficiently reads and processes large CSV files in chunks
- **Data Transformation**: Converts CSV data to structured JSON with appropriate data types
- **Kafka Integration**: Publishes processed data to configured Kafka topics
- **Resumable Processing**: Tracks file read positions to resume after restarts
- **Error Handling**: Robust error handling and retry mechanisms
- **Configuration**: Environment variable based configuration

## Configuration

Configuration is managed through environment variables:

```env
# Kafka Configuration
KAFKA_BROKERS=localhost:9092

# Processing Parameters
POLL_INTERVAL_MS=100
CHUNK_SIZE=65536
BATCH_SIZE=1000

# File Paths (defaults shown)
OFFSET_FILE_PATH=/home/hp/baseServer/offset
```

## Data Processing Pipeline

1. **File Reading**:
   - Files are read in configurable chunks (default: 64KB)
   - Handles partial lines at chunk boundaries
   - Tracks read position for resumable processing

2. **Data Parsing**:
   - CSV parsing with field type conversion
   - Support for various data types (int, float, BigInt, string, boolean)
   - Handles scientific notation for large numbers

3. **Data Transformation**:
   - Maps source CSV fields to target schema
   - Adds unique identifiers and timestamps
   - Validates and cleans data

4. **Publishing**:
   - Batches records for efficient publishing
   - Uses GZIP compression for Kafka messages
   - Handles connection issues with retries

## Kafka Topics

Each processor publishes to a dedicated Kafka topic:

- `csv_ingest_bse` - BSE Cash market data
- `eqd_itrtm_csv` - BSE F&O market data
- `nse_cash_algo` - NSE Cash market data
- `nse_fno_algo` - NSE F&O market data
- `mcx_tradefile` - MCX trade data

## Data Schemas

Each processor defines its own schema in the `HEADER_MAPPING` and `FIELD_TYPES` constants:

- **HEADER_MAPPING**: Maps source CSV column names to target field names
- **FIELD_TYPES**: Specifies the data type for each field (int, float, BigInt, string, boolean)

## Setup and Running

1. Install dependencies:
   ```bash
   npm install kafkajs fast-csv dotenv date-fns
   ```

2. Copy `.env.example` to `.env` and configure:
   ```bash
   cp .env.example .env
   nano .env
   ```

3. Start the desired processor(s):
   ```bash
   node bseCash.js
   node bseFNO.js
   node nseCash.js
   node nseFNO.js
   node mcxtradefile.js
   ```

## Error Handling and Recovery

- Failed Kafka connections are automatically retried
- File read positions are periodically saved to allow for recovery
- Invalid data is logged and skipped
- The application can be safely restarted and will resume from the last known position

## Monitoring

Logs are output to the console and include:
- Connection status
- File processing progress
- Record counts
- Error conditions

## Performance Considerations

- Processing is done in memory-efficient chunks
- Batch publishing reduces network overhead
- GZIP compression minimizes network traffic
- Configurable batch sizes allow tuning for different environments
