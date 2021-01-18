#-*- coding: utf-8 -*-
import jqdata
import datetime
import numpy
from stocktools import *
# 用到回测API请加入下面的语句
from kuanke.user_space_api import *

class StockData:
  def __init__(self, gParam):
    self.id = ''
    self.name = ''
    self.publishDays = 0 # 发行时间
    self.kLineDays = [] # 日k线缓存
    self.preKDJDays = [float(0.00)] * gParam.KDJ_PRE_DAY_COUNT
    self.curKDJDay = float(0.00)
    self.kLineWeeks = [] # 周k线缓存
    self.preKDJWeeks = [float(0.00)] * gParam.KDJ_PRE_WEEK_COUNT
    self.preMacdDiffWeeks = [float(0.00)] * gParam.MACD_DIFF_PRE_WEEK_COUNT
    self.curMacdDiffWeek = float(0.00)
    self.curKDJWeek = float(0.00)
    self.kdjWeekAvgList = [float(0.00)] * gParam.KDJ_WEEK_AVG_COUNT # 实际操作中多缓存一天,减少重复计算,但是平均值只取前x天
    self.kdjWeekAvg = float(0.00)
    self.kLineMonths = [] # 月K线缓存
    self.preMacdDiffMonths = [float(0.00)] * gParam.MACD_PRE_MONTH_COUNT
    self.curMacdDiffMonth = float(0.00)
    self.preKDJMonths = [float(0.00)] * gParam.KDJ_PRE_MONTH_COUNT
    self.curKDJMonth = float(0.00)
    self.kdjMonthAvgList = [float(0.00)] * gParam.KDJ_MONTH_AVG_COUNT # 实际操作中多缓存一天,减少重复计算,但是平均值只取前x天
    self.kdjMonthAvg = float(0.00)

  def preKDJMonthDiff(self, index):
    if index == 0:
      log.info("preKDJMonthDiff index can not be 0")
      return 0
    if index >= len(self.preKDJMonths):
      log.info("preKDJMonthDiff index > len(preKDJMonths)")
      return 0
    if index == 1:
      return self.curKDJMonth - self.preKDJMonths[1]
    return self.preKDJMonths[index-1] - self.preKDJMonths[index]

  def preKDJWeekDiff(self, index):
    if index == 0:
      log.info("preKDJWeekDiff index can not be 0")
      return 0
    if index >= len(self.preKDJWeeks):
      log.info("preKDJWeekDiff index > len(preKDJWeeks)")
      return 0
    if index == 1:
      return self.curKDJWeek - self.preKDJWeeks[1]
    return self.preKDJWeeks[index - 1] - self.preKDJWeeks[index]

  def preKDJDayDiff(self, index):
    if index == 0:
      log.info("preKDJDayDiff index can not be 0")
      return 0
    if index >= len(self.preKDJDays):
      log.info("preKDJDayDiff index > len(preKDJDays)")
      return 0
    if index == 1:
      return self.curKDJDay - self.preKDJDays[1]
    return self.preKDJDays[index - 1] - self.preKDJDays[index]

  def preMACDMonthDiff(self, index):
    if index == 0:
      log.info("preMACDMonthDiff index can not be 0")
      return 0
    if index >= len(self.preMacdDiffMonths):
      log.info("preMACDMonthDiff index > len(preMacdDiffMonths)")
      return 0
    if index == 1:
      return self.curMacdDiffMonth - self.preMacdDiffMonths[1]
    return self.preMacdDiffMonths[index - 1] - self.preMacdDiffMonths[index]

  def preMACDWeekDiff(self, index):
    if index == 0:
      log.info("preMACDWeekDiff index can not be 0")
      return 0
    if index >= len(self.preMacdDiffWeeks):
      log.info("preMACDWeekDiff index > len(preMacdDiffWeeks)")
      return 0
    if index == 1:
      return self.curMacdDiffWeek - self.preMacdDiffWeeks[1]
    return self.preMacdDiffWeeks[index - 1] - self.preMacdDiffWeeks[index]

  def serialPositiveMACDWeekDiffDay(self, days):
    if days <= 0:
      log.info("serialPositiveMACDWeekDiffDay index can not be 0")
      return False
    if days >= len(self.preMacdDiffWeeks):
      log.info("serialPositiveMACDWeekDiffDay index > len(preMacdDiffWeeks)")
      return False
    for index in range(days):
      if self.preMacdDiffWeeks[index] < self.preMacdDiffWeeks[index + 1]:
        return False
    return True
  def serialNegetiveMACDWeekDiffDay(self, days):
    if days <= 0:
      log.info("serialPositiveMACDWeekDiffDay index can not be 0")
      return False
    if days >= len(self.preMacdDiffWeeks):
      log.info("serialPositiveMACDWeekDiffDay index > len(preMacdDiffWeeks)")
      return False
    for index in range(days):
      if self.preMacdDiffWeeks[index] >= self.preMacdDiffWeeks[index + 1]:
        return False
    return True

class DataFactory:
  def __init__(self, gParam):
    self.gParam = gParam
    self.openLog = True


  def GetStockPrice(self, stock, dt):
    if self.gParam.PRODUCT:
      klineList = get_price(
        security=stock,
        count=1, # 这里是天数
        #end_date=datetime.datetime.now().strftime("%Y-%m-%d"),
        end_date=dt.strftime("%Y-%m-%d"),
        frequency="1d",
        fields=['open', 'close', 'high', 'low'],
        skip_paused=True
      )
      return klineList['close'][0] if len(klineList['close']) > 0 else 0
    else:
      klineList = get_bars(security=stock,
        count=1,
        end_dt=dt.strftime("%Y-%m-%d"),
        fields=['open', 'close', 'high', 'low'],
        unit=self.gParam.KLINE_FREQUENCY,
        include_now=True
      )
      return klineList['close'][0] if len(klineList['close']) > 0 else 0

  def genAllStockData(self, securities, cur_datetime, preStockDatas):
    '''
    获取所有股票的当前数据

    Arguments
    ---------
    gParam: 脚本参数
    securities: 股票池
    cur_datetime: 当前日期
    preStockData: 前一天的缓存数据

    Returns: 新计算出的当日stockdata
    -------
    '''
    calcKDJ = CalcKDJ()
    dicStockData = {}
    for stock in securities:
      # 这里取k线柱状图的数据，有一个问题：
      # 由于每次的end_date都是取数据当天，导致每次取到的数据，这里取k线柱状图
      # 的边界都是不一样的（当天往前进行切割）。
      # 这样前后两天执行时的rsi和kdj不一致。可能出现前一天不满足买入卖出条件，但是
      # 第二天一开盘就又满足了条件
      # 这里注意end_date需要传入前一天日期
      pre_date = (cur_datetime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
      klineList = None
      rowIndexList = None # 行索引是时间戳
      if self.gParam.PRODUCT:
        klineList = get_price(
          security=stock,
          count=self.gParam.KLINE_LENGTH * 30, # 这里是天数
          #end_date=datetime.datetime.now().strftime("%Y-%m-%d"),
          end_date=pre_date,
          frequency=self.gParam.KLINE_FREQUENCY,
          fields=['open', 'close', 'high', 'low'],
          skip_paused=True
        )
        # 生产环境的时间戳是当日8点！！ long -> datetime.date类型
        rowIndexList = [datetime.datetime.fromtimestamp(x / 1000000000).date()  for x in klineList._stat_axis.values.tolist()]
      else:
        # 回测环境专用
        # get_bars默认跳过停牌日
        klineList = get_bars(security=stock,
          count=self.gParam.KLINE_LENGTH * 30,
          end_dt=pre_date,
          fields=['date', 'open', 'close', 'high', 'low'],
          unit=self.gParam.KLINE_FREQUENCY,
          include_now=True
        )
        # 回测环境的时间戳是当日0点！！ datetime.date类型
        rowIndexList = klineList['date']

      #print klineList._stat_axis.values.tolist()  # 取行名称
      # columns.values.tolist()   # 取列名称
      publishDays = self.getStockPublishDay(rowIndexList, klineList)
      if publishDays < self.gParam.MIN_PUBLISH_DAYS:
        stockData = StockData(self.gParam)
        stockData.id = stock
        stockData.name = GetStockName(stock)
        stockData.publishDays = publishDays
        dicStockData[stock] = stockData
        continue
      stockData = self.calcStockData(stock, rowIndexList, klineList, -1)
      stockData.name = GetStockName(stock)
      stockData.publishDays = publishDays
      if self.gParam.PRODUCT:
        stockData.kdjMonthAvg = self.getKDJMonthAvg(rowIndexList, klineList, self.gParam.KDJ_MONTH_AVG_COUNT)
        stockData.kdjWeekAvg = self.getKDJWeekAvg(rowIndexList, klineList, self.gParam.KDJ_WEEK_AVG_COUNT)
      else:
        # 测试环境使用缓存的数据计算平均值
        preGStockData = preStockDatas[stock] if preStockDatas and stock in preStockDatas else None
        if preGStockData == None or preGStockData.kdjMonthAvg <= float(0.001):
          # 这里取逆序
          for preDayIndex in range(max(self.gParam.KDJ_MONTH_AVG_COUNT, self.gParam.KDJ_WEEK_AVG_COUNT)-1, -1, -1):
            preDayStockData = self.initStockKlineBar(stock, rowIndexList, klineList, -2 - preDayIndex)
            # 月
            monthIdx = self.gParam.KDJ_MONTH_AVG_COUNT - preDayIndex - 1
            if monthIdx >= 0 and monthIdx < len(stockData.kdjMonthAvgList):
              preDayStockData.preKDJMonths[0] = calcKDJ.GetKDJ(preDayStockData.kLineMonths, self.gParam.KDJ_PARAM1, self.gParam.KDJ_PARAM2, self.gParam.KDJ_PARAM3)[1]
              stockData.kdjMonthAvgList[monthIdx] = preDayStockData.preKDJMonths[0]
            # 周
            weekIdx = self.gParam.KDJ_WEEK_AVG_COUNT - preDayIndex - 1
            if weekIdx >= 0 and weekIdx < len(stockData.kdjWeekAvgList):
              preDayStockData.preKDJWeeks[0] = calcKDJ.GetKDJ(preDayStockData.kLineWeeks, self.gParam.KDJ_PARAM1, self.gParam.KDJ_PARAM2, self.gParam.KDJ_PARAM3)[1]
              stockData.kdjWeekAvgList[weekIdx] = preDayStockData.preKDJWeeks[0]
          stockData.kdjMonthAvg = sum(stockData.kdjMonthAvgList[:self.gParam.KDJ_MONTH_AVG_COUNT]) / self.gParam.KDJ_MONTH_AVG_COUNT
          stockData.kdjMonthAvgList.append(stockData.preKDJMonths[0]) # 把昨日的也缓存起来，省一次计算
          stockData.kdjWeekAvg = sum(stockData.kdjWeekAvgList[:self.gParam.KDJ_WEEK_AVG_COUNT]) / self.gParam.KDJ_WEEK_AVG_COUNT
          stockData.kdjWeekAvgList.append(stockData.preKDJWeeks[0]) # 把昨日的也缓存起来，省一次计算
        else:
          stockData.kdjMonthAvgList = preGStockData.kdjMonthAvgList[1:]
          stockData.kdjMonthAvgList.append(stockData.preKDJMonths[0])
          stockData.kdjMonthAvg = sum(stockData.kdjMonthAvgList[:self.gParam.KDJ_MONTH_AVG_COUNT]) / self.gParam.KDJ_MONTH_AVG_COUNT
          stockData.kdjWeekAvgList = preGStockData.kdjWeekAvgList[1:]
          stockData.kdjWeekAvgList.append(stockData.preKDJWeeks[0])
          stockData.kdjWeekAvg = sum(stockData.kdjWeekAvgList[:self.gParam.KDJ_WEEK_AVG_COUNT]) / self.gParam.KDJ_WEEK_AVG_COUNT
      preDayOpen = klineList['open'][len(rowIndexList) - 1] if len(rowIndexList) > 0 else 0
      preDayClose = klineList['close'][len(rowIndexList) - 1] if len(rowIndexList) > 0 else 0
      if self.openLog == True:
        log.info("id = {id}, pub={publishDays}, pre_open={preDayOpen}, pre_close={preDayClose} \n pre_macd_diff_m={pre_macd_diff_m}, macd_diff_m={macd_diff_m}, pre_macd_diff_w={pre_macd_diff_w}, macd_diff_w={macd_diff_w}, k-4={pre_kdj4},k-3={pre_kdj3}, k-2={pre_kdj2}, k-1={pre_kdj1}, kdj_m = {kdj_m}, kdj_day_mavg = {kdj_day_mavg}, kdj_w = {kdj_w}, kdj_day_wavg = {kdj_day_wavg},kdj_d = {kdj_d}"\
        .format(id = stockData.id, publishDays = stockData.publishDays, \
        preDayOpen = preDayOpen, preDayClose = preDayClose, pre_macd_diff_w=stockData.preMacdDiffWeeks[1], macd_diff_w=stockData.preMacdDiffWeeks[0],\
        pre_macd_diff_m=stockData.preMacdDiffMonths[1], macd_diff_m=stockData.preMacdDiffMonths[0], kdj_m = stockData.preKDJMonths[0], kdj_day_mavg = stockData.kdjMonthAvg, pre_kdj4 = stockData.preKDJMonths[4], pre_kdj3 = stockData.preKDJMonths[3], \
        pre_kdj2 = stockData.preKDJMonths[2], pre_kdj1 = stockData.preKDJMonths[1], kdj_w = stockData.preKDJWeeks[0], kdj_day_wavg = stockData.kdjWeekAvg,kdj_d = stockData.preKDJDays[0]))

      dicStockData[stock] = stockData
    return dicStockData

  # start = -1代表昨日， -2 为前天， 以此类推， 返回的是start当天的数据
  def getStockPublishDay(self, rowIndexList, klineList):
    if len(rowIndexList) == 0:
      return 0
    for idx in range(len(rowIndexList)-1, -1, -1):
      if numpy.isnan(klineList['open'][idx]) or len(rowIndexList)-1-idx >= self.gParam.MIN_PUBLISH_DAYS:
        break
    return len(rowIndexList)-1-idx

  # 获取KDJ前X日的当月平均, 从前天开始
  def getKDJMonthAvg(self, rowIndexList, klineList, count):
    if count == 0:
      return 0
    sumVal = 0.00
    for idx in range(count):
      stockData = self.calcStockData(0, rowIndexList, klineList, -2 - idx)
      sumVal += stockData.preKDJMonths[0]
    return sumVal / float(count)

  def getKDJWeekAvg(self, rowIndexList, klineList, count):
    if count == 0:
      return 0
    sumVal = 0.00
    for idx in range(count):
      stockData = self.calcStockData(0, rowIndexList, klineList, -2 - idx)
      sumVal += stockData.preKDJWeeks[0]
    return sumVal / float(count)

  def calcStockData(self, stockId, rowIndexList, klineList, start):
    calcKDJ = CalcKDJ()
    calcMACD = CalcMACD()
    stockData = self.initStockKlineBar(stockId, rowIndexList, klineList, start)
    # Month
    for n in range(self.gParam.KDJ_PRE_MONTH_COUNT):
      stockData.preKDJMonths[n] = calcKDJ.GetKDJ(stockData.kLineMonths[n:], self.gParam.KDJ_PARAM1, self.gParam.KDJ_PARAM2, self.gParam.KDJ_PARAM3)[1]
    for n in range(self.gParam.MACD_PRE_MONTH_COUNT):
      stockData.preMacdDiffMonths[n] = calcMACD.GetDiff(stockData.kLineMonths[n:])
    stockData.curKDJMonth = stockData.preKDJMonths[0]
    stockData.curMacdDiffMonth = stockData.preMacdDiffMonths[0]
    # week
    for n in range(self.gParam.KDJ_PRE_WEEK_COUNT):
      stockData.preKDJWeeks[n] = calcKDJ.GetKDJ(stockData.kLineWeeks[n:], self.gParam.KDJ_PARAM1, self.gParam.KDJ_PARAM2, self.gParam.KDJ_PARAM3)[1]
    for n in range(self.gParam.MACD_DIFF_PRE_WEEK_COUNT):
      stockData.preMacdDiffWeeks[n] = calcMACD.GetDiff(stockData.kLineWeeks[n:])
    stockData.curKDJWeek = stockData.preKDJWeeks[0]
    stockData.curMacdDiffWeek = stockData.preMacdDiffWeeks[0]
    # day
    for n in range(self.gParam.KDJ_PRE_DAY_COUNT):
      stockData.preKDJDays[n] = calcKDJ.GetKDJ(stockData.kLineDays[n:], self.gParam.KDJ_PARAM1, self.gParam.KDJ_PARAM2, self.gParam.KDJ_PARAM3)[1]
    stockData.curKDJDay = stockData.preKDJDays[0]
    return stockData

  def initStockKlineBar(self, stockId, rowIndexList, klineList, start):
    stockData = StockData(self.gParam)
    stockData.id = stockId
    # 按自然月和周划分k线柱
    kLineMonth = KLineBar()
    kLineWeek = KLineBar()
    kLineDay = KLineBar()
    #print(len(klineList._stat_axis.values.tolist()))
    # 从昨日开始往前遍历数据
    for idx in range(len(rowIndexList)+start, -1, -1):
        if numpy.isnan(klineList['open'][idx]):
            # 已经取到未上市的日期，后面的不再取了
            if kLineMonth != None:
                stockData.kLineMonths.append(kLineMonth)
            if kLineWeek != None:
                stockData.kLineWeeks.append(kLineWeek)
            if kLineDay != None:
                stockData.kLineDays.append(kLineDay)
            break
        time_date = rowIndexList[idx]
        pre_timedate = time_date
        if idx + 1 < len(rowIndexList):
            # 这里取下一天的比较
            pre_timedate = rowIndexList[idx + 1]
        if self.gParam.KLINE_SPLIT_TYPE == SPLIT_KLINE_FIXED_DAY:
          # 跨月，而且上月有数据，结算上一个k线图数据
          if kLineMonth and kLineMonth.day >= self.gParam.KLINE_BAR_MONTH_DAY:
            stockData.kLineMonths.append(kLineMonth)
            kLineMonth = None
          # 跨周，而且上周有数据
          if kLineWeek and kLineWeek.day >= self.gParam.KLINE_BAR_WEEK_DAY:
            stockData.kLineWeeks.append(kLineWeek)
            kLineWeek = None
        elif self.gParam.KLINE_SPLIT_TYPE == SPLIT_KLINE_NORMAL:
          # 跨自然月
          if kLineMonth and time_date.month != pre_timedate.month:
            stockData.kLineMonths.append(kLineMonth)
            kLineMonth = None
          # 跨自然周
          if kLineWeek and time_date.isocalendar()[1] != pre_timedate.isocalendar()[1]:
            stockData.kLineWeeks.append(kLineWeek)
            kLineWeek = None

        # 每天都是跨天
        kLineDay = None

        if kLineMonth == None:
            kLineMonth = KLineBar()
        if kLineWeek == None and len(stockData.kLineWeeks) < self.gParam.KLINE_LENGTH:
            kLineWeek = KLineBar()
        if kLineDay == None and len(stockData.kLineDays) < self.gParam.KLINE_LENGTH:
            kLineDay = KLineBar()

        k_open = klineList['open'][idx]
        k_close = klineList['close'][idx]
        k_high = klineList['high'][idx]
        k_low = klineList['low'][idx]

        kLineMonth.UpdatePreDayData(k_open, k_close, k_high, k_low)
        if kLineWeek != None:
          kLineWeek.UpdatePreDayData(k_open, k_close, k_high, k_low)
        # 更新日数据
        if kLineDay != None:
          kLineDay.UpdatePreDayData(k_open, k_close, k_high, k_low)
          stockData.kLineDays.append(kLineDay)

    if len(stockData.kLineMonths) < self.gParam.KLINE_LENGTH and len(stockData.kLineMonths) > 0:
        lastKline = stockData.kLineMonths[-1]
        for i in range(self.gParam.KLINE_LENGTH - len(stockData.kLineMonths)):
            kLineMonth = KLineBar()
            kLineMonth.open = lastKline.open
            kLineMonth.close = lastKline.close
            kLineMonth.high = lastKline.high
            kLineMonth.low = lastKline.low
            stockData.kLineMonths.append(kLineMonth)
    return stockData