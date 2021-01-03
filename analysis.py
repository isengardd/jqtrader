# 导入函数库
import jqdata
import time
import datetime
import math
import calendar
import numpy
from stocktools import *

class TraderParam:
  def __init__(self):
    # analysis特有参数
    self.MARKET_CAP_FILT = float(100) # 市值过滤
    self.PE_RATIO = float(25) # 市盈率过滤
    self.PB_RATIO = float(10) # 市净率过滤
    # self.TOTAL_OPERATING_REVENUE = float(200) # 营业总收入过滤 (200亿)
    # self.INCOME = 2e8 # 利润

    # 与trader复用的参数
    self.KLINE_FREQUENCY = "1d"
    self.KLINE_LENGTH = 60       # 月K线数量， 最多取 60个月数据

    self.RSI_PARAM = 5
    self.KDJ_PARAM1 = 9
    self.KDJ_PARAM2 = 3
    self.KDJ_PARAM3 = 3

gParam = TraderParam()

class StockData:
  def __init__(self):
    self.name = ''
    self.id = ''
    self.klines = []
    self.publishDays = 0 # 发行时间
    self.preRSI = float(0.00)
    self.preMacdDiff = float(0.00)
    self.preMacdDiff_1 = float(0.00)
    self.preMacdDiff_2 = float(0.00)
    self.preMacdDiff_3 = float(0.00)
    self.preMacdDiff_4 = float(0.00)
    self.preKDJ = float(0.00)
    self.preKDJ_1 = float(0.00)
    self.preKDJ_2 = float(0.00)
    self.preKDJ_3 = float(0.00)
    self.preKDJ_4 = float(0.00)
    self.curRSI = float(0.00)
    self.curKDJ = float(0.00)
    self.curMacdDiff = float(0.00)

# 初始化函数，设定要操作的股票、基准等等
def initialize(context):
  # log.info(g.securities)
  gParam = TraderParam()

  g.securities = get_all_securities(['stock'])._stat_axis.values.tolist()
  set_universe(g.securities)
  # 设定沪深300作为基准
  set_benchmark('000300.XSHG')
  # 开启动态复权(真实价格)模式
  set_option('use_real_price', True)

def handle_data(context, data):
  pass

def before_trading_start(context):
  # 初始化 rsi 和 kdj 数据
  calcRSI = CalcRSI()
  calcKDJ = CalcKDJ()
  calcMACD = CalcMACD()

  validCount = 0
  log.info("total stock: {count}".format(count=len(g.securities)))
  for stock in g.securities:
    pre_date = (context.current_dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    klineList = None
    rowIndexList = None # 行索引是时间戳

    # 先获取财务数据
    q = query(valuation).filter(
      valuation.code == stock
    )
    df = get_fundamentals(q)
    if len(df['market_cap']) == 0:
      continue
    if not int(df['market_cap'][0]) > gParam.MARKET_CAP_FILT:
      continue
    if not int(df['pe_ratio'][0]) < gParam.PE_RATIO:
      continue
    # pb_ratio

    # 回测环境专用
    # get_bars默认跳过停牌日
    klineList = get_bars(security=stock,
      count=gParam.KLINE_LENGTH * 30,
      fields=['date', 'open', 'close', 'high', 'low'],
      unit=gParam.KLINE_FREQUENCY,
      include_now=False
    )
    # 回测环境的时间戳是当日0点！！ datetime.date类型
    rowIndexList = klineList['date']
    stockData = StockData()
    securityInfo = get_security_info(stock)
    stockData.name = securityInfo.display_name
    stockData.id = stock
    # 按自然月划分k线柱
    dealWithFirstMonth = False
    kline = None
    #print(len(klineList._stat_axis.values.tolist()))
    for idx in range(len(rowIndexList)-1, -1, -1):
        if numpy.isnan(klineList['open'][idx]):
            # 已经取到未上市的日期，后面的不再取了
            if kline != None:
                stockData.klines.append(kline)
            break
        if kline == None:
            kline = KLineBar()
        time_date = rowIndexList[idx]
        pre_timedate = time_date
        if idx > 0:
            pre_timedate = rowIndexList[idx - 1]
        k_open = klineList['open'][idx]
        k_close = klineList['close'][idx]
        k_high = klineList['high'][idx]
        k_low = klineList['low'][idx]
        # 更新本月数据
        kline.open = k_open
        if kline.close < float(0.001):
            kline.close = k_close
        if k_high > kline.high:
            kline.high = k_high
        if k_low < kline.low or kline.low < float(0.001):
            kline.low = k_low

        # print "idx={5} hopen: {0}  close: {1}  timestamp: {2} high: {3} low: {4}".format(kline.open, kline.close, kline.timestamp, kline.high, kline.low, idx)
        # 最后的一月的K柱，取的是本月和上一个月的数据，减少数据波动
        if idx == 0 or time_date.month != pre_timedate.month:
            # 最近一个月的与前一个月合并，相当于两个月一条k柱
            if len(stockData.klines) == 1 and not dealWithFirstMonth:
                firstKline = stockData.klines[0]
                firstKline.open = kline.open
                if firstKline.high < kline.high:
                    firstKline.high = kline.high
                if firstKline.low > kline.low:
                    firstKline.low = kline.low
                dealWithFirstMonth = True
            else:
                stockData.klines.append(kline)
            kline = None
    stockData.publishDays = len(rowIndexList)-1-idx

    if len(stockData.klines) < gParam.KLINE_LENGTH and len(stockData.klines) > 0:
        lastKline = stockData.klines[-1]
        for i in range(gParam.KLINE_LENGTH - len(stockData.klines)):
            kline = KLineBar()
            kline.open = lastKline.open
            kline.close = lastKline.close
            kline.high = lastKline.high
            kline.low = lastKline.low
            stockData.klines.append(kline)

    # print len(stockData.klines)
    # stockData.preRSI = calcRSI.GetRSI(stockData.klines, gParam.RSI_PARAM)
    #print "rsi = {0}".format(stockData.preRSI)
    stockData.preKDJ = calcKDJ.GetKDJ(stockData.klines, gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
    stockData.preKDJ_4 = calcKDJ.GetKDJ(stockData.klines[4:], gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
    stockData.preKDJ_3 = calcKDJ.GetKDJ(stockData.klines[3:], gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
    stockData.preKDJ_2 = calcKDJ.GetKDJ(stockData.klines[2:], gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
    stockData.preKDJ_1 = calcKDJ.GetKDJ(stockData.klines[1:], gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
    stockData.preMacdDiff = calcMACD.GetDiff(stockData.klines)
    # stockData.preMacdDiff_4 = calcMACD.GetDiff(stockData.klines)
    # stockData.preMacdDiff_3 = calcMACD.GetDiff(stockData.klines)
    # stockData.preMacdDiff_2 = calcMACD.GetDiff(stockData.klines)
    stockData.preMacdDiff_1 = calcMACD.GetDiff(stockData.klines[1:])
    stockData.curRSI = stockData.preRSI
    stockData.curKDJ = stockData.preKDJ
    stockData.curMacdDiff = stockData.preMacdDiff
    k_open = stockData.klines[0].open if len(stockData.klines) > 0 else 0
    k_close = stockData.klines[0].close if len(stockData.klines) > 0 else 0
    preDayOpen = klineList['open'][len(rowIndexList) - 1] if len(rowIndexList) > 0 else 0
    preDayClose = klineList['close'][len(rowIndexList) - 1] if len(rowIndexList) > 0 else 0
    if stockData.curKDJ < 70.00 and stockData.publishDays >= 24 * 19:
      buyReason = 0
      buyMsg = ""
      diff_4 = stockData.preKDJ_3 - stockData.preKDJ_4
      diff_3 = stockData.preKDJ_2 - stockData.preKDJ_3
      diff_2 = stockData.preKDJ_1 - stockData.preKDJ_2
      diff_1 = stockData.curKDJ - stockData.preKDJ_1
      if diff_1 >= 0.5 and diff_2 < 0 and diff_3 < 0:
        buyReason = 1
        buyMsg = "diff_1 >= 0.5 and diff_2 < 0 and diff_3 < 0"
      elif diff_1 + diff_2 >= 0.5 and diff_2 > 0 and diff_2 < 0.5 and diff_3 < 0 and diff_4 < 0:
        buyReason = 2
        buyMsg = "diff_1 + diff_2 >= 0.5 and diff_2 > 0 and diff_2 < 0.5 and diff_3 < 0 and diff_4 < 0"
      elif diff_4 > 0 and diff_3 > 0 and diff_2 < 0 and diff_1 > 0.5:
        buyReason = 3

      if buyReason > 0 and stockData.preMacdDiff_1 < stockData.preMacdDiff:
        validCount += 1
        log.info("name= {name}, id = {id}, number={number},pub={publishDays}, pre_open={preDayOpen}, pre_close={preDayClose}, k_open={k_open}, k_close={k_close}, \n reason={buyReason},rsi = {rsi}, pre_macd_diff={pre_macd_diff}, macd_diff={macd_diff}, k-4={pre_kdj4},k-3={pre_kdj3}, k-2={pre_kdj2}, k-1={pre_kdj1}, kdj = {kdj}"\
        .format(name = stockData.name, id = stockData.id, number=validCount, publishDays = stockData.publishDays, k_open = k_open, \
        k_close = k_close, preDayOpen = preDayOpen, preDayClose = preDayClose, \
        buyReason = buyReason, rsi = stockData.preRSI, pre_macd_diff = stockData.preMacdDiff_1, macd_diff = stockData.preMacdDiff, kdj = stockData.preKDJ, pre_kdj4 = stockData.preKDJ_4, pre_kdj3 = stockData.preKDJ_3, \
        pre_kdj2 = stockData.preKDJ_2, pre_kdj1 = stockData.preKDJ_1))