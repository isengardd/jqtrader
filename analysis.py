# 导入函数库
import jqdata
import time
import datetime
import math
import calendar
import numpy
from stocktools import *
from datafactory import *

class TraderParam:
  def __init__(self):
    # analysis特有参数
    self.MARKET_CAP_FILT = float(150) # 市值过滤
    self.PE_RATIO = float(35) # 市盈率过滤
    self.PB_RATIO = float(10) # 市净率过滤
    # self.TOTAL_OPERATING_REVENUE = float(200) # 营业总收入过滤 (200亿)
    # self.INCOME = 2e8 # 利润

    # 交易参数
    self.PRODUCT = False # 是否是生产环境
    self.MULTI_STATUS_MACHINE = False # 是否使用多层状态机
    self.KLINE_FREQUENCY = "1d"
    self.KLINE_LENGTH = 150       # 月K线数量， 最多取 60个月数据
    self.MIN_PUBLISH_DAYS = 28 * 5 # 最少上市天数
    self.ROOM_MAX = 10 # 要交易的股票数
    self.BUY_INTERVAL_DAY = 1
    self.SELL_INTERVAL_DAY = 1
    self.SH_DEAD_KDJ_LINE = 75.00  # 上证指数kdj超过这个数值，停止交易，卖出所有持仓
    self.SH_STOP_BUY_KDJ_LINE = 73.00 # 上证指数kdj超过这个数值，停止买入
    self.ONE_STOCK_BUY_KDJ_LINE = 85.00 # 个股的kdj超过这个数值，停止买入

    self.RSI_PARAM = 5
    self.KDJ_PARAM1 = 9
    self.KDJ_PARAM2 = 3
    self.KDJ_PARAM3 = 3
    self.KDJ_PRE_MONTH_COUNT = 5 # KDJ月线缓存数
    self.KDJ_PRE_WEEK_COUNT = 5 # KDJ周线缓存数
    self.KDJ_PRE_DAY_COUNT = 5 # KDJ日线缓存数
    self.KDJ_MONTH_AVG_COUNT = 40 # KDJ每日月均线缓存数（前X天的月KDJ列表,用于计算平均值）
    self.KDJ_WEEK_AVG_COUNT = 10 # KDJ每日周均线缓存数
    self.MACD_PRE_MONTH_COUNT = 2 # MACD月线缓存数
    self.MACD_DIFF_PRE_WEEK_COUNT = 10 # MACD_DIFF周线缓存数
    self.MACD_DEA_PRE_WEEK_COUNT = 5 # MACD_DEA周线缓存
    # k线参数
    self.KLINE_SPLIT_TYPE = SPLIT_KLINE_NORMAL
    self.KLINE_BAR_MONTH_DAY = 20 # k线月线的天数
    self.KLINE_BAR_WEEK_DAY = 5 # k线周线的天数

gParam = TraderParam()

class AnalysticTool:
  def __init__(self):
    self.stocks = []
    self.win = 0
    self.loss = 0

# 初始化函数，设定要操作的股票、基准等等
def initialize(context):
  # log.info(g.securities)
  gParam = TraderParam()
  
  g.analysTool = AnalysticTool()
  # 去除688开头科创板
  g.securities = [x for x in get_all_securities(['stock'])._stat_axis.values.tolist() if not x.startswith('688')]
  set_universe(g.securities)
  # 设定沪深300作为基准
  set_benchmark('000300.XSHG')
  # 开启动态复权(真实价格)模式
  set_option('use_real_price', True)

def handle_data(context, data):
  pass

def log_stock_buy(stockData):
  log.info("id={id}, name={name}, price={cur_price}".format(id=stockData.id, name=stockData.name ,cur_price=stockData.kLineDays[0].close))

def log_stock_sell(buyStockData, sellStockData):
  loss = buyStockData.kLineDays[0].close >= sellStockData.kLineDays[0].close
  if (loss):
    g.analysTool.loss += 1
  else:
    g.analysTool.win += 1
  log.info("win={winCount}, loss={lossCount}, trading={tradeCount}".format(winCount=g.analysTool.win, lossCount=g.analysTool.loss, tradeCount=len(g.analysTool.stocks)))
  winMsg = "*--" if not loss else ""
  log.info(winMsg + "id={id}, name={name}, buyDate={buyDate}, sellDate={sellDate}, diffDate={diffDate}, buyPrice={buyPrice}, sellPrice={sellPrice}".format(\
    id=buyStockData.id, name=buyStockData.name, buyDate=buyStockData.kLineDays[0].endTime, sellDate=sellStockData.kLineDays[0].endTime, \
    diffDate=(sellStockData.kLineDays[0].endTime - buyStockData.kLineDays[0].endTime).days, buyPrice=buyStockData.kLineDays[0].close, sellPrice=sellStockData.kLineDays[0].close))

def before_trading_start(context):
  # 初始化 rsi 和 kdj 数据
  dataFactory = DataFactory(gParam)
  dataFactory.openLog = False

  validCount = 0
  log.info("total stock: {count}".format(count=len(g.securities)))
  for stock in g.securities:
    # 去掉st
    stockName = GetStockName(stock)
    if stockName.startswith('ST') or stockName.startswith('*ST'):
      continue
    # 去除科创板（需要50万资产）
    if stockName.startswith('688'):
      continue

    # 去除上市时间过少的股票 （这里包含未开盘日）
    fullStartDays = GetStockStartDays(stock, context.current_dt.date())
    if fullStartDays < gParam.MIN_PUBLISH_DAYS:
      continue

    # 先获取财务数据
    q = query(valuation).filter(
      valuation.code == stock
    )
    df = get_fundamentals(q)
    if len(df['market_cap']) == 0:
      continue
    market_cap = int(df['market_cap'][0])
    if market_cap < gParam.MARKET_CAP_FILT:
      continue
    pe_ratio = df['pe_ratio'][0]
    if pe_ratio > gParam.PE_RATIO or pe_ratio <= 0:
      continue

    # 回测环境专用
    dicStockData = dataFactory.genAllStockData([stock], context.current_dt, None)
    if stock in dicStockData and stock not in [x.id for x in g.analysTool.stocks]:
      stockData = dicStockData[stock]
      # 至少上市56周
      if stockData == None or stockData.publishDays < gParam.MIN_PUBLISH_DAYS:
        continue

      #if pe_ratio < 5:
      #  log.info("id={id}, name={name}, pe={pe_ratio}".format(id=stock, name=stockData.name, pe_ratio=pe_ratio))

      # 月线上涨，周线背离（股价下降，周kdj上涨）买入
      # 周线顶背离（股价上升，周kdj下降，或者周kdj大于80，周线下降）
      if stockData.preKDJMonths[0] <= 85 and stockData.serialPositiveKDJMonth(2):
        if stockData.kLineWeeks[1].open * 0.93 > stockData.kLineWeeks[1].close and stockData.serialPositiveKDJWeek(2):
          log_stock_buy(stockData)
          g.analysTool.stocks.append(stockData)
      # 周线小于20
      #if stockData.preKDJWeeks[0] <= 20.0 and \
      #  stockData.preKDJWeeks[0] > stockData.preKDJWeeks[1] and \
      #  stockData.serialPositiveMACDWeekDiff(2):
      #  log.info("id={id}, name={name}, week_kdj={w_kdj}, pre_kdj={w_prekdj}".format(id=stock, name=stockData.name, w_kdj=stockData.preKDJWeeks[0], w_prekdj=stockData.preKDJWeeks[1]))
      # 月线平均上升
      # if stockData.preKDJMonths[0] <= 65 and \
      #   stockData.kdjMonthAvgList[0] > stockData.kdjMonthAvgList[1] and \
      #   stockData.kdjMonthAvgList[2] > stockData.kdjMonthAvgList[1]:
      #   curPrice = stockData.kLineDays[0].close
      #   twoMonthPrice = dataFactory.GetStockPrice(stock, datetime.datetime(2020, 12, 6))
      #   log.info("id={id}, name={name}, price={cur_price}, twoMonth_price={price_2}".format(id=stock, name=stockData.name, cur_price=curPrice, price_2=twoMonthPrice))
  removeDatas = []
  for buyStockData in g.analysTool.stocks:
    dicStockData = dataFactory.genAllStockData([buyStockData.id], context.current_dt, None)
    if buyStockData.id in dicStockData:
      stockData = dicStockData[buyStockData.id]
      if (stockData.kLineWeeks[0].close * 0.97 > stockData.kLineWeeks[0].open and stockData.serialNegetiveKDJWeek(1)) or \
        stockData.serialNegetiveKDJWeek(1):
        log_stock_sell(buyStockData, stockData)
        removeDatas.append(buyStockData)
  for rmData in removeDatas:
    g.analysTool.stocks.remove(rmData)
