import pandas as pd
import pandas_ta as ta
import json
import datetime
import yfinance as yf
from json import JSONEncoder


class Stock:

    def __init__(self, symbol, currentPrice, period):
        self.technicals = None
        self.symbol = symbol
        self.currentPrice = currentPrice
        self.derivatives = None
        self.period = period

    def addTechnicals(self, technicals):
        self.technicals = technicals

    def addDerivatives(self, derivatives):
        self.derivatives = derivatives


class Derivatives:
    def __init__(self, optionChain):
        self.optionChain = optionChain


class StockEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


class OptionChain:

    def __init__(self, expiry, type, atm, iv, ivRank, dte, bid, ask, avg, volume, oi):
        self.expiry = expiry
        self.type = type
        self.atm = atm
        self.iv = iv
        self.ivRank = ivRank
        self.dte = dte
        self.bid = bid
        self.ask = ask
        self.avg = avg
        self.volume = volume
        self.oi = oi


class Technicals:
    def __init__(self, timeFrame, rsi, signal):
        self.timeFrame = timeFrame
        self.ema = []
        self.rsi = rsi
        self.signal = signal

    def addEma(self, em):
        self.ema.append(em)
        return self


class EMA:
    def __init__(self, days, value):
        self.days = days
        self.value = value


def handler(event, context):
    data = []
    df = pd.DataFrame()
    tickers = ["AAPL", "SPY", "^IXIC", "^GSPC", "^VIX"]
    interval = "1h"
    period = "6mo"
    isOptionChain = False
    expiryRange = "8"
    if event is not None:
        if event.get('queryStringParameters'):
            if event['queryStringParameters'].get('tickers'):
                tickers = event['queryStringParameters']['tickers'].split(",")

        if event.get('queryStringParameters'):
            if event['queryStringParameters'].get('interval'):
                interval = event['queryStringParameters']['interval']

        if event.get('queryStringParameters'):
            if event['queryStringParameters'].get('period'):
                period = event['queryStringParameters']['period']

        if event.get('queryStringParameters'):
            if event['queryStringParameters'].get('isOptionChain'):
                isOptionChain = event['queryStringParameters']['isOptionChain']

        if event.get('queryStringParameters'):
            if event['queryStringParameters'].get('expiryRange'):
                expiryRange = event['queryStringParameters']['expiryRange']

    for ticker in tickers:
        data.append(processTicker(df, ticker, interval, period, expiryRange, isOptionChain))
    #final_data = "{\"data\":" + json.dumps(data, cls=StockEncoder, indent=4) + "}"
        final_data = {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps(data, cls=StockEncoder),
            "isBase64Encoded": False
        }
    print(final_data)
    return final_data


def processTicker(df, symbol, interval, period, expiryRange, isOptionChain):
    df = df.ta.ticker(symbol, interval=interval, period=period)
    pd.set_option('display.max_columns', None)
    if df is not None:
        rsi = ta.rsi(df["Close"])
        #print(df.shape[0])
        frameLength = df.shape[0]
        emaSlow = ta.ema(df["Close"], length=50)
        emaFast = ta.ema(df["Close"], length=14)
        emaMid = ta.ema(df["Close"], length=21)
        emaSlowest = ta.ema(df["Close"], length=200)
        df = pd.concat([df, rsi, emaFast, emaMid, emaSlow, emaSlowest], axis=1)
        df = df.round(2)
        df["BEAR"] = df["EMA_14"] < df["EMA_21"]
        df["BULL"] = df["EMA_14"] > df["EMA_21"]
        rating = "SELL" if (df["BEAR"].values[-1] and df["RSI_14"].values[-1] < 49) else "BUY" \
            if (df["BULL"].values[-1] and df["RSI_14"].values[-1] > 49) else "NEU"
        ema14 = str(df["EMA_14"].values[-1])
        ema21 = str(df["EMA_21"].values[-1])
        ema50 = str(df["EMA_50"].values[-1])
        rsi = str(df["RSI_14"].values[-1])
        if frameLength >= 200:
            ema200 = str(df["EMA_200"].values[-1])
        else:
            ema200 = 0

        currentPrice = str(df["Close"].values[-1])

        return prepareNode(symbol, interval, currentPrice, period, ema14, ema21, rating, rsi, ema50, ema200,
                           expiryRange, isOptionChain)
    else:
        return Stock(symbol, None, None)


def prepareNode(symbol, interval, currentPrice, period, ema14, ema21, rating, rsi, ema50, ema200, expiryRange,
                isOptionChain):
    EMA14 = EMA(14, ema14)
    EMA21 = EMA(21, ema21)
    EMA50 = EMA(50, ema50)
    EMA200 = EMA(200, ema200)
    EMAs = list()
    EMAs.append(EMA14)
    EMAs.append(EMA21)
    EMAs.append(EMA50)
    EMAs.append(EMA200)
    technicals = Technicals(interval, rsi, rating)
    technicals.addEma(EMA14).addEma(EMA21).addEma(EMA50).addEma(EMA200)
    stock = Stock(symbol, currentPrice, period)
    stock.addTechnicals(technicals)
    if isOptionChain:
        stock.addDerivatives(processDerivatives(symbol, expiryRange))

    return stock


def processDerivatives(symbol, expiryRange):
    optionChains = list()
    optionsDf = options_chain(symbol, expiryRange)
    optionsDf = optionsDf.reset_index()

    for row in optionsDf.itertuples(index=True, name='Pandas'):
        optionChains.append(
            OptionChain(row.expirationDate.strftime('%m/%d/%Y'), "CALL" if row.CALL else "PUT", row.strike,
                        row.impliedVolatility, None, row.dte,
                        row.bid, row.ask, row.mark, row.volume, row.openInterest))
    return optionChains


def toJSON(self):
    return json.dumps(self, default=lambda o: o.__dict__)


# df_final = df_final.concat(tickers)


def options_chain(symbol, expiryRange):
    tk = yf.Ticker(symbol)
    currentPrice = tk.info["regularMarketPrice"]
    # Expiration dates
    exps = tk.options

    # Get options for each expiration
    options = pd.DataFrame()
    for i in range(1, int(expiryRange)):
        opt = tk.option_chain(exps[i])
        optC = pd.DataFrame().append(opt.calls)
        optC['expirationDate'] = exps[i]
        optCalls = optC.iloc[(optC['strike'] - currentPrice).abs().argsort()[:1]]

        optP = pd.DataFrame().append(opt.puts)
        optP['expirationDate'] = exps[i]
        optPuts = optP.iloc[(optP['strike'] - currentPrice).abs().argsort()[:1]]
        options = options.append(optPuts, ignore_index=True)
        options = options.append(optCalls, ignore_index=True)

    # Bizarre error in yfinance that gives the wrong expiration date
    # Add 1 day to get the correct expiration date
    options['expirationDate'] = pd.to_datetime(options['expirationDate'])
    options['dte'] = (options['expirationDate'] - datetime.datetime.today()).dt.days
    options['impliedVolatility'] = options['impliedVolatility'] * 100
    # Boolean column if the option is a CALL
    options['CALL'] = options['contractSymbol'].str[4:].apply(
        lambda x: "C" in x)

    options[['bid', 'ask', 'strike']] = options[['bid', 'ask', 'strike']].apply(pd.to_numeric)
    options['mark'] = (options['bid'] + options['ask']) / 2
    # options ['IVRank'] =   # Calculate the midpoint of the bid-ask

    # Drop unnecessary and meaningless columns
    options = options.drop(
        columns=['contractSymbol', 'contractSize', 'currency', 'change', 'percentChange', 'lastTradeDate', 'lastPrice',
                 'inTheMoney'])
    pd.set_option('display.max_columns', None)
    # print(options)
    return options


# options_chain("AAPL")
#handler(None, None)
