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
    self.MARKET_CAP_FILT = float(100) # 市值过滤
    self.PE_RATIO = float(25) # 市盈率过滤
    self.PB_RATIO = float(10) # 市净率过滤
    # self.TOTAL_OPERATING_REVENUE = float(200) # 营业总收入过滤 (200亿)
    # self.INCOME = 2e8 # 利润

    # 交易参数
    self.PRODUCT = False # 是否是生产环境
    self.MULTI_STATUS_MACHINE = False # 是否使用多层状态机
    self.KLINE_FREQUENCY = "1d"
    self.KLINE_LENGTH = 60       # 月K线数量， 最多取 60个月数据
    self.MIN_PUBLISH_DAYS = 24 * 19 # 最少上市天数
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
    self.MACD_PRE_WEEK_COUNT = 10 # MACD周线缓存数
    # k线参数
    self.KLINE_BAR_MONTH_DAY = 40 # k线月线的天数
    self.KLINE_BAR_WEEK_DAY = 10 # k线周线的天数

gParam = TraderParam()

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
  dataFactory = DataFactory(gParam)

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

    # 回测环境专用
    dicStockData = dataFactory.genAllStockData([stock], context, None)
    if stock in dicStockData:
      stockData = dicStockData[stock]
      # 周线小于20
      if stockData.preKDJWeeks[0] <= 20.0:
        log.info("id={id}, name={name}, week_kdj={w_kdj}".format(id=stock, name=stockData.name, w_kdj=stockData.preKDJWeeks[0]))