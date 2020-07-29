const ccxt = require("ccxt");
const logger = require("./winston")(module);
const { InfluxDB, Point, HttpError } = require("@influxdata/influxdb-client");
const { url, token, org, listedSources } = require("./env");
const argv = require('yargs').argv

const queryApi = new InfluxDB({ url, token }).getQueryApi(org);
const args = process.argv.slice(2);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const flush = (writeApi) => {
  writeApi
    .flush()
    .then(() => {
      logger.info("Batch saved...");
    })
    .catch((e) => {
      logger.error(e);
      if (e instanceof HttpError && e.statusCode === 401) {
        logger.info("Setup an InfluxDB database!");
      }
      logger.warn("\nFinished ERROR");
    });   
}

const getParams = (exchange, since) => {
  switch(exchange) {
    case 'bitmex': 
      return {
        startTime: since,
        count: 750,
      }
    case 'bitfinex':
      return {
        start: since.getTime(),
        limit: 500
      }
    default:
      return {}
  }
}

const fetchOHLCV = async (exchangeName, symbol, tf, since = 0, writeApi) => {
  try {
    if (since === undefined)
      throw 'Can not fetch data; invalid starting date';
    since = new Date(since);
    const now = new Date();
    const exchange = new ccxt[exchangeName] ();
    const params = getParams(exchangeName, since);
    while (since < now) {
      let partial = await exchange.fetchOHLCV(symbol, tf, null, null, params);
      logger.info(`Found ${partial.length} OHLCV datapoints in ${exchangeName} for ${symbol}`);
      let lastTs = 0;
      for (const e of partial) {
        if (e.indexOf(undefined) !== -1) continue;
        const [ts, open, high, low, close, volume] = e;
        lastTs = new Date(ts);
        const p = new Point(`ohlcv`)
          .tag("symbol", symbol)
          .tag("tf", tf)
          .floatField("open", open)
          .floatField("high", high)
          .floatField("low", low)
          .floatField("close", close)
          .floatField("volume", volume)
          .timestamp(lastTs);
        writeApi.writePoint(p);
      }      
      logger.info(`Successful fetch from ${since} to ${lastTs}`)
      flush(writeApi);   
      if (partial.length <= 2)
        break;
      since = new Date(partial[partial.length - 1][0]);
      await sleep(exchange.rateLimit);
    }
  } catch (err) {
    logger.error(err);
  }
}

const getLastOHLCVTimestamp = (exchange, symbol, tf) =>
  new Promise((resolve, reject) => {
    let lastTimestamp;
    const fluxQuery = `
      from(bucket:"${exchange}")
        |> range(start:0)
        |> filter(fn: (r) => r._measurement == "ohlcv" and
          r.symbol == "${symbol}" and
          r.tf == "${tf}" and
          r._field == "close")
        |> last()`
    queryApi.queryRows(fluxQuery, {
      next(row, tableMeta) {
        const o = tableMeta.toObject(row);
        lastTimestamp = new Date(o._time);
      },
      error(err) {
        reject(err);
      },
      complete() {
        resolve(lastTimestamp);
      },
    });
  });

const queryExchange = async (exchangeName) => {
  const exchange = new ccxt[exchangeName] ();
  await exchange.loadMarkets();
  logger.info(`Symbols available at ${exchangeName}:`)
  logger.info(`Symbols ${exchange.symbols.join(', ')}`)
}

const main = async () => {
  try {
    let since;
    let sources = listedSources;
    
    if (argv.query) {
      queryExchange(argv.query)
      return;
    }
    if (argv.lastTime) {
      const [exchange, symbol, tf] = argv.lastTime.split('-');
      const lastTimestamp = await getLastOHLCVTimestamp(exchange, symbol, tf)
      logger.info(`${exchange}-${symbol}-${tf} was updated last time on ${lastTimestamp}`)
      return;
    }
    if (argv.origin)
      since = 0
    if (argv.year)
      since = new Date(`${argv.year}, 1, 1`)
    if (argv._.length > 0)
      sources = argv._;

    for (const source of sources) {
      const [exchangeName, symbol, tf] = source.split('-');
      if (since === undefined)
        since = await getLastOHLCVTimestamp(exchangeName, symbol, tf);
      if (since === undefined)
        since = 0;

      logger.info(`Updating OHLCV data in ${exchangeName}, for ${symbol} and timeframe ${tf} since ${since}`);
      const writeApi = new InfluxDB({ url, token }).getWriteApi(org, exchangeName, "ms");
      await fetchOHLCV(exchangeName, symbol, tf, since, writeApi);
      writeApi
        .close()
        .then(() => {
          logger.info("All data saved.");
        })
        .catch((e) => {
          logger.error(e);
          if (e instanceof HttpError && e.statusCode === 401) {
            logger.info("Setup an InfluxDB database!");
        }
        logger.warn("\nFinished with Errors");
      });
    }
  } catch (err) {
    logger.error(err);
  }
};

main();
