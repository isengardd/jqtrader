#-*- coding: utf-8 -*-
import jqdata
import time
import datetime
import math
# 用到回测API请加入下面的语句
from kuanke.user_space_api import *

EMA_K_FACTOR = 5.4500     # ema公式中的k值系数
ERR_DATA = -666666        # 错误数据

SKILL_MACD = 0
SKILL_RSI = 1
SKILL_KDJ = 2
SKILL_AVG = 3

# 分割kline的方法
SPLIT_KLINE_NORMAL = 1 #按自然日期月，周
SPLIT_KLINE_FIXED_DAY = 2 #按固定天数

SH_CODE = '000001.XSHG' # 上证股票代码

def ObjAttrs(obj):
  return '  '.join(['{0}: {1}'.format(item[0], item[1]) for item in obj.__dict__.items()])

def LowStockCount(num):
  return int(num/100.00)*100

def GetDayTimeStamp(dt, deltaDay):
  # 获取当天起始时间戳
  tp = datetime.datetime(year=dt.year, month=dt.month, day=dt.day, hour=0, minute=0, second=0).timetuple()
  tt = time.mktime(tp) + deltaDay * 86400
  return int(tt)

def GetDayTimeStr(dt, deltaDay):
  return (dt + datetime.timedelta(days=deltaDay)).strftime("%Y-%m-%d")

def GetStockName(stock_id):
  securityInfo = get_security_info(stock_id)
  return securityInfo.display_name

def IsSameDay(d1, d2):
  return d1.year==d2.year and d1.month==d2.month and d1.day==d2.day

def GetStockStart(stock_id):
  securityInfo = get_security_info(stock_id)
  return securityInfo.start_date #datetime.date

# 获取上市到指定时间的天数，包括未开盘天（周末，停盘等）
def GetStockStartDays(stock_id, tg_date):
  start_dt = GetStockStart(stock_id)
  return (tg_date - start_dt).days

class TimeRecord:
  def __init__(self):
    self.startTime = 0
    self.endTime = 0

  def start(self):
    self.startTime = time.time()
    self.endTime = 0

  def end(self, msg):
    self.endTime = time.time()
    log.info("{msg} time: {diffTime}".format(msg=msg, diffTime=self.endTime-self.startTime))

class CalcCommon:
  def __init__(self):
    self.emaFactorMap = {}
    self.skillType = SKILL_MACD

  def GetEMA(self, valueList, N):
    if len(valueList) == 0:
      log.info("error: GetEMA: N = {N}, but valueList is empty".format(N = N))
      return None
    # minLen = self.GetEMA_K(N)
    # if len(valueList) < minLen:
    #   log.info("GetEMA: N = {0}, minLen = {1}, len(val) = {2}".format(N, minLen, len(valueList)))
    #   return float(0)
    emaFactorList = self.GetEMAFactorList(N)
    #if len(emaFactorList) > len(valueList):
    #  emaFactorList = self.GetEMAFactorList(int(len(valueList) / EMA_K_FACTOR - 1))
    if len(emaFactorList) == 0:
      log.info("GetEMA: len(emaFactorList) == 0")
      return None

    alpha = self.GetAlpha(N)
    ema = float(0.0000)
    for i in range(len(emaFactorList)):
      val = float(valueList[i]) if len(valueList) > i else valueList[len(valueList) - 1]
      ema += (float(val) * emaFactorList[i])

    ema = ema*alpha
    return ema

  def GetEMA_K(self, N):
    return int(float(EMA_K_FACTOR) * float(N + 1))

  def GetAlpha(self, N):
    if self.skillType == SKILL_MACD:
      return float(2.0000) / float(N+1)
    else:
      return float(1.0000) / float(N)

  def GetEMAFactorList(self, N):
    if N in self.emaFactorMap:
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

class CalcAvg(CalcCommon):
  def __init__(self):
    CalcCommon.__init__(self)
    self.skillType = SKILL_AVG

  def GetAvg(self, kLine, N):
    if N == 0:
      return ERR_DATA
    
    totalDay = 0
    sumPrice = 0.0000
    for i in range(len(kLine)):
      if i < N:
        sumPrice += kLine[i].close
        totalDay += 1
      else:
        break
    return sumPrice / totalDay

class CalcKDJ(CalcCommon):
  def __init__(self):
    CalcCommon.__init__(self)
    self.skillType = SKILL_KDJ

  def GetKDJ(self, kLine, N, M1, M2):
    if N == 0 or M1 == 0 or M2 == 0:
      return (ERR_DATA, ERR_DATA)
    # 这里只返回 (k,d)
    rsvDay = int(self.GetEMA_K(M2) + self.GetEMA_K(M1)) + int(N)
    if rsvDay > len(kLine):
        rsvDay = len(kLine)
    defaultKDJ = 50.0000
    if rsvDay <= 1:
      return defaultKDJ, defaultKDJ
    rsvList = [float(0) for i in range(rsvDay)]
    for i in range(rsvDay):
      rsvList[i] = self.GetRSV(kLine[i : min(int(N) + i, len(kLine))], N)
    # 算出K值
    kDay = int(self.GetEMA_K(M2))
    if kDay < 2:
      log.info("GetKDJ: kDay < 2, M2 = {0}".format(M2))
      return (ERR_DATA, ERR_DATA)
    kList = [float(0) for i in range(kDay)]
    if len(rsvList) < len(kList):
      kList[len(kList) - 1] = defaultKDJ
    else:
      kList[len(kList)-1] = self.GetEMA(rsvList[len(kList)-1 : min(len(rsvList), len(kList)+int(self.GetEMA_K(M1))-1)], M1)
      if kLine[len(kList)-1] is None:
        kLine[len(kList)-1] = defaultKDJ
    for i in range(len(kList)-2, -1, -1):
      if i < len(rsvList) - 1:
        kList[i] = self.GetAlpha(M1)*rsvList[i] + kList[i+1]*(1-self.GetAlpha(M1))
      else:
        kList[i] = defaultKDJ
    # 算出d值
    dVal = self.GetEMA(kList, M2)
    #if len(kLine) == 2:
    #  log.info([ObjAttrs(obj) for obj in kLine])
    #  log.info(kList)
    #  log.info(rsvList)
    return (round(kList[0], 4), round(dVal, 4))
  def GetRSV(self, kLine, N):
    if len(kLine) < N:
      if len(kLine) == 0:
        log.info("GetRSV error, len(kLine) = {0} < N = {1}".format(len(kLine), N))
        return ERR_DATA
      else:
        N = len(kLine)

    max = self.GetMaxPrice(kLine[:N])
    min = self.GetMinPrice(kLine[:N])

    if max == ERR_DATA or min == ERR_DATA or min == max:
      log.info("GetRSV error, min = {min}, max = {max}".format(min=min,max=max))
      return ERR_DATA

    # print "max = {0}, min = {1}".format(max, min)
    rsv = (kLine[0].close - min) / (max - min) * float(100.0000)
    if rsv < float(0.0001):
      rsv = float(0.0001)

    # if rsv > float(100.0000):
    #   rsv = float(100.0000)

    return rsv
  def CalcEMAFactorList(self, N):
    pass

class CalcRSI(CalcCommon):
  def __init__(self):
    CalcCommon.__init__(self)
    self.skillType = SKILL_RSI

  def GetRSI(self, kLine, N):
    if len(kLine) == 0:
      return ERR_DATA
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
      return ERR_DATA
    # print "uEma = {0}, dEma = {1}".format(uEma, dEma)
    return (uEma / (uEma + dEma)) * float(100.0000)

  def CalcEMAFactorList(self, N):
    pass

class CalcMACD(CalcCommon):
  def __init__(self):
    CalcCommon.__init__(self)
    self.skillType = SKILL_MACD

  def GetDiff(self, kLine):
    if len(kLine) == 0:
      return ERR_DATA
    closeKline = [i.close for i in kLine]
    ema_12 = self.GetEMA(closeKline, 12)
    if ema_12 == None:
      return ERR_DATA
    ema_26 = self.GetEMA(closeKline, 26)
    if ema_26 == None:
      return ERR_DATA
    return ema_12 - ema_26

  def GetDEA(self, diffLine):
    return self.GetEMA(diffLine, 9)

class KLineBar:
  def __init__(self):
    self.open = float(0.00)
    self.close = float(0.00)
    self.high = float(0.00)
    self.low = float(0.00)
    self.endTime = None #datetime.datetime(2020, 11, 30).date()
    self.day = 0 # 统计的天数

  def UpdatePreDayData(self, open, close, high, low):
    self.open = open
    if self.close < float(0.001):
        self.close = close
    if high > self.high:
        self.high = high
    if low < self.low or self.low < float(0.001):
        self.low = low
    self.day += 1
