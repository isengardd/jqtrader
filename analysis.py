# 导入函数库
import jqdata
import time
import datetime
import math
import calendar
import numpy

class TraderParam:
  def __init__(self):
    # analysis特有参数
    self.MARKET_CAP_FILT = float(100) # 市值过滤
    self.PE_RATIO = float(25) # 市盈率过滤
    self.PB_RATIO = float(10) # 市净率过滤
    # self.TOTAL_OPERATING_REVENUE = float(200) # 营业总收入过滤 (200亿)
    # self.INCOME = 2e8 # 利润

    # 与trader复用的参数
    self.EMA_K_FACTOR = 5.4500     # ema公式中的k值系数
    self.ERR_DATA = -666666        # 错误数据
    self.KLINE_FREQUENCY = "1d"
    self.KLINE_LENGTH = 60       # 月K线数量， 最多取 60个月数据
    self.SH_CODE = '000001.XSHG'

    self.SKILL_MACD = 0
    self.SKILL_RSI = 1
    self.SKILL_KDJ = 2
    self.RSI_PARAM = 5
    self.KDJ_PARAM1 = 9
    self.KDJ_PARAM2 = 3
    self.KDJ_PARAM3 = 3

gParam = TraderParam()

def LowStockCount(num):
  return int(num/100.00)*100

def GetDayTimeStamp(dt, deltaDay):
  # 获取当天起始时间戳
  tp = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=0, minute=0, second=0).timetuple()
  tt = time.mktime(tp) + deltaDay * 86400
  return int(tt)

class CalcCommon:
  def __init__(self):
    self.emaFactorMap = {}
    self.skillType = gParam.SKILL_MACD

  def GetEMA(self, valueList, N):
    if len(valueList) == 0:
      log.info("error: GetEMA: N = {N}, but valueList is empty".format(N = N))
      return float(0)
    # minLen = self.GetEMA_K(N)
    # if len(valueList) < minLen:
    #   log.info("GetEMA: N = {0}, minLen = {1}, len(val) = {2}".format(N, minLen, len(valueList)))
    #   return float(0)

    emaFactorList = self.GetEMAFactorList(N)
    if len(emaFactorList) == 0:
      log.info("GetEMA: len(emaFactorList) == 0")
      return float(0)
    alpha = self.GetAlpha(N)
    ema = float(0)
    for i in range(len(emaFactorList)):
      val = float(valueList[i]) if len(valueList) > i else 0
      ema += (float(val) * emaFactorList[i])

    ema = ema*alpha
    return ema

  def GetEMA_K(self, N):
    return int(float(gParam.EMA_K_FACTOR) * float(N + 1))

  def GetAlpha(self, N):
    if self.skillType == gParam.SKILL_MACD:
      return float(2) / float(N+1)
    else:
      return float(1) / float(N)

  def GetEMAFactorList(self, N):
    if self.emaFactorMap.has_key(N):
      return self.emaFactorMap[N]
    if N <= 0:
      return []

    alpha = self.GetAlpha(N)
    k = self.GetEMA_K(N)
    emaFactorList = [1.0000]
    for i in range(1, k):
      emaFactorList.append(float(emaFactorList[i-1])*(float(1) - alpha))
    self.emaFactorMap[N] = emaFactorList
    return self.emaFactorMap[N]

  def GetMaxPrice(self, kLine):
    max = float(0)
    for val in kLine:
      if val.high > max:
        max = val.high
    return max

  def GetMinPrice(self, kLine):
    min = float(10000000)
    for val in kLine:
      if val.low < min:
        min = val.low
    return min

class CalcKDJ(CalcCommon):
  def __init__(self):
    CalcCommon.__init__(self)
    self.skillType = gParam.SKILL_KDJ

  def GetKDJ(self, kLine, N, M1, M2):
    if N == 0 or M1 == 0 or M2 == 0:
      return (gParam.ERR_DATA, gParam.ERR_DATA)
    # 这里只返回 (k,d)
    rsvDay = int(self.GetEMA_K(M2) + self.GetEMA_K(M1)) + int(N)
    if len(kLine) < rsvDay:
      log.info("GetKDJ: len(kLine) = {0}, rsvDay = {1}".format(len(kLine), rsvDay))
      return (gParam.ERR_DATA, gParam.ERR_DATA)
    rsvList = [float(0) for i in range(rsvDay)]
    for i in range(rsvDay):
      rsvList[i] = self.GetRSV(kLine[i : int(N) + i], N)
    # 算出K值
    kDay = int(self.GetEMA_K(M2))
    if kDay < 2:
      log.info("GetKDJ: kDay < 2, M2 = {0}".format(M2))
      return (gParam.ERR_DATA, gParam.ERR_DATA)
    kList = [float(0) for i in range(kDay)]
    kList[len(kList)-1] = self.GetEMA(rsvList[len(kList)-1 : len(kList)+int(self.GetEMA_K(M1))-1], M1)
    for i in range(len(kList)-2, -1, -1):
      if math.isnan(kList[i+1]):
        kList[i+1] = 50.00
      kList[i] = self.GetAlpha(M1)*rsvList[i] + kList[i+1]*(1-self.GetAlpha(M1))
    # 算出d值
    dVal = self.GetEMA(kList, M2)
    # print kList
    # print rsvList
    return (round(kList[0], 2), round(dVal, 2))
  def GetRSV(self, kLine, N):
    if len(kLine) < N:
      if len(kLine) == 0:
        log.info("GetRSV error, len(kLine) = {0} < N = {1}".format(len(kLine), N))
        return gParam.ERR_DATA
      else:
        N = len(kLine)

    max = self.GetMaxPrice(kLine[:N])
    min = self.GetMinPrice(kLine[:N])

    if max == gParam.ERR_DATA or min == gParam.ERR_DATA or min == max:
      log.info("GetRSV error, min = {min}, max = {max}".format(min=min,max=max))
      return gParam.ERR_DATA

    # print "max = {0}, min = {1}".format(max, min)
    rsv = (kLine[0].close - min) / (max - min) * float(100)
    if rsv < float(1):
      rsv = float(1)

    if rsv > float(100):
      rsv = float(100)

    return rsv
  def CalcEMAFactorList(self, N):
    pass

class CalcRSI(CalcCommon):
  def __init__(self):
    CalcCommon.__init__(self)
    self.skillType = gParam.SKILL_RSI

  def GetRSI(self, kLine, N):
    if len(kLine) == 0:
      return gParam.ERR_DATA
    uKline = [float(0) for i in kLine]
    dKline = [float(0) for i in kLine]

    for i in range(len(kLine) - 1):
      if kLine[i].close > kLine[i+1].close:
        uKline[i] = kLine[i].close - kLine[i+1].close
      elif kLine[i].close < kLine[i+1].close:
        dKline[i] = kLine[i+1].close - kLine[i].close
    uEma = self.GetEMA(uKline, N)
    dEma = self.GetEMA(dKline, N)
    if uEma == 0 or dEma == 0:
      return gParam.ERR_DATA
    # print "uEma = {0}, dEma = {1}".format(uEma, dEma)
    return (uEma / (uEma + dEma)) * float(100.0000)

  def CalcEMAFactorList(self, N):
    pass

class CalcMACD(CalcCommon):
  def __init__(self):
    CalcCommon.__init__(self)
    self.skillType = gParam.SKILL_MACD

  def GetDiff(self, kLine):
    closeKline = [i.close for i in kLine]
    return self.GetEMA(closeKline, 12) - self.GetEMA(closeKline, 26)

class KLineBar:
  def __init__(self):
    self.open = float(0.00)
    self.close = float(0.00)
    self.high = float(0.00)
    self.low = float(0.00)
    self.timestamp = 0

class StockData:
  def __init__(self):
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
        log.info("id = {id}, number={number},pub={publishDays}, pre_open={preDayOpen}, pre_close={preDayClose}, k_open={k_open}, k_close={k_close}, \n reason={buyReason},rsi = {rsi}, pre_macd_diff={pre_macd_diff}, macd_diff={macd_diff}, k-4={pre_kdj4},k-3={pre_kdj3}, k-2={pre_kdj2}, k-1={pre_kdj1}, kdj = {kdj}"\
        .format(id = stockData.id, number=validCount, publishDays = stockData.publishDays, k_open = k_open, \
        k_close = k_close, preDayOpen = preDayOpen, preDayClose = preDayClose, \
        buyReason = buyReason, rsi = stockData.preRSI, pre_macd_diff = stockData.preMacdDiff_1, macd_diff = stockData.preMacdDiff, kdj = stockData.preKDJ, pre_kdj4 = stockData.preKDJ_4, pre_kdj3 = stockData.preKDJ_3, \
        pre_kdj2 = stockData.preKDJ_2, pre_kdj1 = stockData.preKDJ_1))