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
    self.MIN_BUY_COUNT = 100       # 最小买股数

    # 交易参数
    self.PRODUCT = True # 是否是生产环境
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
    # 枚举
    self.PROCESS_NONE = 0
    self.PROCESS_BUY = 1
    self.PROCESS_BUY_DONE = 2
    self.PROCESS_SELL = 3
    self.PROCESS_SELL_DONE = 4
    self.PROCESS_SUB_BUY = 5
    self.PROCESS_SUB_BUY_DONE = 6
    self.PROCESS_SUB_SELL = 7
    self.PROCESS_SUB_SELL_DONE = 8

    self.ORDER_REBUY = 1
    self.ORDER_RESELL = 2

    self.securities = [
    SH_CODE, # 上证指数也考虑进来
    '601988','000538','000002','600642',
    '600104','601633','000895','600660',
    '600690','000568','600031','600320',
    '601933','002415','600763','000651',
    '603288','600276','002294',
    '600887','600030','601668',
    '601919','000725','002352','601628',
    '601319','002797'
    ] if self.PRODUCT else [
      SH_CODE, '600763'
    ]
gParam = TraderParam()

class StockData:
  def __init__(self):
    self.id = ''
    self.publishDays = 0 # 发行时间
    self.kLineDays = [] # 日k线缓存
    self.preKDJDays = [float(0.00)] * gParam.KDJ_PRE_DAY_COUNT
    self.curKDJDay = float(0.00)
    self.kLineWeeks = [] # 周k线缓存
    self.preKDJWeeks = [float(0.00)] * gParam.KDJ_PRE_WEEK_COUNT
    self.preMacdDiffWeeks = [float(0.00)] * gParam.MACD_PRE_WEEK_COUNT
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

class TradeManager:   # 交易管理
    def __init__(self):
        self.rooms = [] #交易席位
        self.cashTotal = float(0.00)   # 账户总资金
        self.cashLeft = float(0.00)    # 账户剩余资金
        self.cashFree = float(0.00)    # 账户可使用资金(扣除了room中锁定的资金)
        self.runCount = 0 # 执行run的次数

    def hasEmptyRoom(self):
        return len(self.rooms) < g.MAX_ROOM

    def run(self, context, data):
        recalcKDJ = False
        if self.runCount == 1:
            # 离收盘差5分钟时再执行一次
            if context.current_dt.hour == 14 and context.current_dt.minute >= 55:
                recalcKDJ = True
            else:
                return
        if self.runCount >= 2:
            # log.info("TradeManager daily runcount = {0}".format(self.runCount))
            return
        self.runCount += 1

        shData = g.stockDatas[normalize_code(SH_CODE)]
        calcKDJ = CalcKDJ()
        calcMACD = CalcMACD()
        if recalcKDJ:
            for stockId in g.securities:
                # if normalize_code(SH_CODE) == stockId:
                #     continue

                if data[stockId].paused == True:
                    continue

                # 收盘前5分钟，更新一下当日的kdj
                # 更新今日的K线数据
                cur_price = data[stockId].avg
                stockData = g.stockDatas[stockId]
                if len(stockData.kLineMonths) > 0:
                  if cur_price > stockData.kLineMonths[0].high:
                    stockData.kLineMonths[0].high = cur_price
                  elif cur_price < stockData.kLineMonths[0].low:
                    stockData.kLineMonths[0].low = cur_price
                  stockData.kLineMonths[0].close = cur_price
                  stockData.curKDJMonth = calcKDJ.GetKDJ(stockData.kLineMonths, gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
                  stockData.curMacdDiffMonth = calcMACD.GetDiff(stockData.kLineMonths)
                if len(stockData.kLineWeeks) > 0:
                  if cur_price > stockData.kLineWeeks[0].high:
                    stockData.kLineWeeks[0].high = cur_price
                  elif cur_price < stockData.kLineWeeks[0].low:
                    stockData.kLineWeeks[0].low = cur_price
                  stockData.kLineWeeks[0].close = cur_price
                  stockData.curKDJWeek = calcKDJ.GetKDJ(stockData.kLineWeeks, gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
                  stockData.curMacdDiffWeek = calcMACD.GetDiff(stockData.kLineWeeks)
                if len(stockData.kLineDays) > 0:
                  if cur_price > stockData.kLineDays[0].high:
                    stockData.kLineDays[0].high = cur_price
                  elif cur_price < stockData.kLineDays[0].low:
                    stockData.kLineDays[0].low = cur_price
                    stockData.kLineDays[0].close = cur_price
                  stockData.curKDJDay = calcKDJ.GetKDJ(stockData.kLineDays, gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
        # 上证周macd与前一周差值连续7天为正
        if len(self.rooms) < g.MAX_ROOM and shData.curKDJMonth < gParam.SH_STOP_BUY_KDJ_LINE:
            # 席位未满,查找买入机会
            roomsId = [room.id for room in self.rooms]
            for stockId in g.securities:
                if normalize_code(SH_CODE) == stockId:
                    continue
                if data[stockId].paused == True:
                    continue
                if stockId in roomsId:
                    continue
                stockData = g.stockDatas[stockId]
                if stockData.publishDays < gParam.MIN_PUBLISH_DAYS:
                    continue

                cur_price = data[stockId].avg
                roomCash = self.getNewRoomCash(context)
                # 资金不够操作
                if roomCash < cur_price * gParam.MIN_BUY_COUNT:
                  log.info("not enough money to buy, roomCash={roomCash}, minRequire={mincash}".format(roomCash=roomCash,mincash=cur_price * gParam.MIN_BUY_COUNT))
                  continue

                if stockData.curKDJMonth == ERR_DATA:
                  log.info("data error,id = {id} curKDJ = {curKDJ}".format(curKDJ = stockData.curKDJMonth, id = stockData.id))
                  continue

                monthDiff1 = stockData.preKDJMonthDiff(1)
                weekDiff1 = stockData.preKDJWeekDiff(1)
                dayDiff1 = stockData.preKDJDayDiff(1)
                dayDiff2 = stockData.preKDJDayDiff(2)
                # 1. 上市天数大于24个月
                # 2. 如果kdj D值的月线上升，而且diff月线也上升，判定为可买入
                # 3. 如果周线上涨，在日线底部反转点买入
                if stockData.curKDJMonth < gParam.ONE_STOCK_BUY_KDJ_LINE:
                  buyReason = 0
                  buyMsg = ""
                  # if monthDiff1 > 0 and monthDiff2 < 0 and monthDiff3 < 0 and monthMacdDiff > 0:
                  #   buyReason = 1
                  #   buyMsg = "monthDiff1 > 0 and monthDiff2 < 0 and monthDiff3 < 0 and monthMacdDiff > 0 and weekDiff1 > 0 and weekMacdDiff > 0"
                  if stockData.serialPositiveMACDWeekDiffDay(7) and stockData.curKDJMonth > stockData.kdjMonthAvg + 2:
                    buyReason = 2
                    buyMsg = "stockData.curMacdDiffMonth > 1.00 and stockData.curKDJMonth > stockData.kdjMonthAvg + 2"
                  if buyReason > 0:
                    # 符合买入条件，进入交易席位
                    newRoom = TradeRoom()
                    newRoom.id = stockData.id
                    newRoom.cashTotal = roomCash
                    newRoom.cashLeft = roomCash
                    newRoom.tradeProcess = TradeProcess()
                    newRoom.tradeProcess.tradeType = gParam.PROCESS_BUY
                    newRoom.tradeProcess.stepEnable = GetDayTimeStamp(context.current_dt, 0)
                    self.rooms.append(newRoom)
                    log.info("enter room, stockid={stockid}, preKDJ_1={preKDJ_1}, curKDJ={curKDJ}, lockCash={lockCash}".format(stockid = stockData.id, preKDJ_1 = stockData.preKDJMonths[1], curKDJ = stockData.curKDJMonth, lockCash = roomCash))
                    log.info("buyReason: {buyReason}, buyMsg = {buyMsg}".format(buyReason = buyReason, buyMsg = buyMsg))
                    log.info("monthDiff1={monthDiff1},monthMacdDiff={monthMacdDiff},weekDiff1={weekDiff1},dayDiff2={dayDiff2},dayDiff1={dayDiff1}".format(monthDiff1=monthDiff1,monthMacdDiff=stockData.curMacdDiffMonth,weekDiff1=weekDiff1,dayDiff2=dayDiff2,dayDiff1=dayDiff1))
                    if len(self.rooms) < g.MAX_ROOM:
                        continue
                    else:
                        break
        # 先把要删除的房间删除
        for room in self.rooms:
            room.beforeRun(context, data)
        delRooms = []
        for room in self.rooms:
            if room.finished():
                delRooms.append(room)
        for room in delRooms:
            log.info("room finished, stockid={0}".format(room.id))
            self.rooms.remove(room)

        for room in self.rooms:
            room.run(context, data)

    def getNewRoomCash(self, context):
        if len(self.rooms) >= g.MAX_ROOM:
            return float(0.00)
        
        return float((context.portfolio.available_cash - self.getRoomLockCash()) / (g.MAX_ROOM - len(self.rooms)))
    
    def getRoomLockCash(self):
        roomLockCash = 0
        for room in self.rooms:
            roomLockCash += room.cashLeft
        return roomLockCash
        
class TradeRoom:    #交易席位
    def __init__(self):
        self.id = ''
        self.tradeProcess = None
        self.cashTotal = float(0.00)
        self.cashLeft = float(0.00)
        self.stockCount = int(0) # 股票数
        self.tradeOrder = None

    def finished(self):
        return self.tradeProcess.tradeType == gParam.PROCESS_SELL_DONE

    def beforeRun(self, context, data):
        # 处理从gParam.PROCESS_SELL到gParam.PROCESS_SELL_DONE的过程
        if self.tradeProcess.tradeType == gParam.PROCESS_SELL:
            # 先查询现有订单状态
            if self.tradeOrder != None:
                ordersDic = get_orders(order_id=self.tradeOrder.id)
                if self.tradeOrder.id in ordersDic:
                    cur_order = ordersDic[self.tradeOrder.id]
                    if cur_order.status == OrderStatus.held:
                        log.info("sell order held, stockid={0}, orderid={1}".format(self.id, self.tradeOrder.id))
                        self.tradeProcess.changeType(context, gParam.PROCESS_SELL_DONE)
                        self.tradeOrder = None  # 交易成功，清除交易对象
                        return
                    else:
                        return
                else:
                    log.info("sell order not found, stockid={0}, orderid={1}".format(self.id, self.tradeOrder.id))
                    return
    def run(self, context, data):
        shData = g.stockDatas[normalize_code(SH_CODE)]
        if shData.curKDJMonth >= gParam.SH_DEAD_KDJ_LINE:
            self.tradeProcess.changeType(context, gParam.PROCESS_SELL)

        # 如果有前一天未执行完的订单，优先继续执行
        if self.dealYestdayOrder(data):
          return

        if self.tradeProcess.tradeType == gParam.PROCESS_BUY:
          self.processBuy(context, data)
        if self.tradeProcess.tradeType == gParam.PROCESS_BUY_DONE:
          self.processBuyDone(context)
        if self.tradeProcess.tradeType == gParam.PROCESS_SELL:
          self.processSell(context, data)

    def dealYestdayOrder(self, data):
      if self.tradeOrder != None and (self.tradeOrder.status == gParam.ORDER_RESELL or self.tradeOrder.status == gParam.ORDER_REBUY):
        log.info("redo yesterday order, stockid={0}, orderid={1}, status={2}".format(self.id, self.tradeOrder.id, self.tradeOrder.statuString()))
        orderCount = self.tradeOrder.stockCount
        if orderCount == 0:
          log.info("error, redo order stockcount is empty!")
          self.tradeOrder = None
          return False
        shData = g.stockDatas[normalize_code(SH_CODE)]
        if shData.curKDJMonth >= gParam.SH_DEAD_KDJ_LINE:
          self.tradeOrder = None
          return False

        if self.tradeOrder.status == gParam.ORDER_RESELL:
          orderCount = -orderCount
        cur_price = data[self.id].avg
        orderRes = order(self.id, orderCount)
        if orderRes == None:
          log.info("error, redo order failed: orderRes is none, id={0},price={1},cashLeft={2}".format(self.id, cur_price, self.cashLeft))
          return False
        self.tradeOrder.id = orderRes.order_id
        self.tradeOrder.status = 0
        self.tradeOrder.stockCount = 0
        return True

    def updateStockCount(self, context):
      if self.id in context.subportfolios[0].positions:
        position = context.subportfolios[0].positions[self.id]
        if self.stockCount != position.total_amount:
          self.stockCount = position.total_amount
          log.info("update stockcount, stock_id={0}, stockcount={1}".format(self.id, self.stockCount))
      else:
        self.stockCount = 0

    def monthDecideSell(self, context):
      stockData = g.stockDatas[self.id]
      if stockData.curKDJMonth == ERR_DATA:
        log.info("data error,id = {id} curKDJ = {curKDJ}".format(curKDJ=stockData.curKDJMonth, id=stockData.id))
        return False

      sellReason = 0
      sellMsg = ""
      # 月线反转，判定为卖出
      if stockData.curKDJMonth < stockData.kdjMonthAvg - 4.50 or stockData.serialNegetiveMACDWeekDiffDay(7):
        sellReason = 1
        sellMsg = "stockData.curKDJMonth < stockData.kdjMonthAvg - 4.50"
      if sellReason > 0:
        self.tradeProcess.changeType(context, gParam.PROCESS_SELL)
        log.info("change to sell, stockid={stockid}, preKDJ_1={preKDJ_1}, curKDJ={curKDJ}".format(stockid = self.id, preKDJ_1 = stockData.preKDJMonths[1], curKDJ = stockData.curKDJMonth))
        log.info("sellReason: {sellReason}, Msg = {sellMsg}".format(sellReason = sellReason, sellMsg = sellMsg))
        return True
      return False

    def updateOrBuyOrder(self, context, data):
      # 先查询现有订单状态
      if self.tradeOrder != None:
          ordersDic = get_orders(order_id=self.tradeOrder.id)
          if self.tradeOrder.id in ordersDic:
              cur_order = ordersDic[self.tradeOrder.id]
              if cur_order.status == OrderStatus.held:
                  log.info("buy order held, stockid={0}, orderid={1}".format(self.id, self.tradeOrder.id))
                  if gParam.MULTI_STATUS_MACHINE:
                    self.tradeProcess.changeSubType(gParam.PROCESS_SUB_BUY_DONE)
                  else:
                    self.tradeProcess.changeType(context, gParam.PROCESS_BUY_DONE)
                  self.tradeOrder = None  # 交易成功，清除交易对象
                  self.cashLeft = 0
                  return
          else:
              log.info("buy order not found, stockid={0}, orderid={1}".format(self.id, self.tradeOrder.id))
              return
      if GetDayTimeStamp(context.current_dt, 0) >= self.tradeProcess.stepEnable:
          # 如果当前已经有买单
          if self.tradeOrder != None:
              return
          cur_price = data[self.id].avg
          buy_count = LowStockCount(self.cashLeft / cur_price)
          if buy_count == 0:
              log.info("error,buy_count=0: id={0},price={1},cashleft={2}".format(self.id, cur_price, self.cashLeft))
              return
          orderRes = order(self.id, buy_count)
          if orderRes == None:
              log.info("error, buy order failed: orderRes is none, id={0},price={1},cashleft={2}".format(self.id, cur_price, self.cashLeft))
              return

          self.tradeOrder = TradeOrder()
          self.tradeOrder.id = orderRes.order_id
          log.info("order buy: stock_id={0}, order_id={1}, price={2}".format(self.id, orderRes.order_id, cur_price))

    def processBuy(self, context, data):
      if gParam.MULTI_STATUS_MACHINE == False:
        # 单层模式
        self.updateOrBuyOrder(context, data)
      else:
        # 1. 更新持股数
        # 2. 如果月线判定为卖出，转为卖出状态
        # 3. 否则进入周线逻辑
        self.updateStockCount(context)
        if self.monthDecideSell(context):
          return

        # todo: 根据周线交易
        self.processSubTrade(context, data)

    def processBuyDone(self, context):
      self.cashLeft = 0
      self.updateStockCount(context)
      if self.stockCount == 0:
        log.info("trade in gParam.PROCESS_BUY_DONE, stock_id={0} but stockCount is 0".format(self.id))
        self.tradeProcess.changeType(context, gParam.PROCESS_SELL)
        return
      self.monthDecideSell(context)

    def processSell(self, context, data):
      # todo: 如果有买单，需要撤销
      if self.tradeOrder == None:
        self.updateStockCount(context)

        if self.stockCount == 0:
          log.info("trade in gParam.PROCESS_SELL, stockid={0}  but stockCount is 0".format(self.id))
          self.tradeProcess.changeType(context, gParam.PROCESS_SELL_DONE)
          return

        if GetDayTimeStamp(context.current_dt, 0) >= self.tradeProcess.stepEnable:
          sell_count = self.stockCount
          if sell_count == 0:
            self.tradeProcess.changeType(context, gParam.PROCESS_SELL_DONE)
            return

          cur_price = data[self.id].avg
          orderRes = order(self.id, -sell_count)
          if orderRes == None:
            log.info("error, sell order failed: orderRes is none, id={0},price={1},stockCount={2}".format(self.id, cur_price, self.stockCount))
            return

          self.tradeOrder = TradeOrder()
          self.tradeOrder.id = orderRes.order_id
          log.info("order sell: stock_id={0}, order_id={1}, price={2}".format(self.id, orderRes.order_id, cur_price))

    def processSubTrade(self, context, data):
      stockData = g.stockDatas[self.id]
      if self.tradeProcess.subTradeType == gParam.PROCESS_NONE:
        self.cashLeft = self.cashTotal
        if stockData.curKDJMonth >= gParam.ONE_STOCK_BUY_KDJ_LINE:
          return
        # 超过周线Kdj均线，切换到买入状态
        # 资金不够操作
        cur_price = data[self.id].avg
        minRequireCash = cur_price * gParam.MIN_BUY_COUNT
        if self.cashLeft < minRequireCash:
          log.info("processSubTrade not enough money to buy, roomCash={roomCash}, minRequire={mincash}".format(roomCash=self.cashLeft,mincash=minRequireCash))
          return
        if stockData.curKDJWeek == ERR_DATA:
          log.info("data error,id = {id} curKDJWeek = {curKDJ}".format(curKDJ = stockData.curKDJWeek, id = stockData.id))
          return
        buyReason = 0
        buyMsg = ""
        if stockData.curMacdDiffWeek > 0 and stockData.curKDJWeek > stockData.kdjWeekAvg + 2.5:
          buyReason = 1
          buyMsg = "stockData.curMacdDiffWeek > 0 and stockData.curKDJWeek > stockData.kdjWeekAvg + 0.5"
        if buyReason > 0:
          log.info("processSubTrade change to buy, stockId={id}, macddiff={macddiff}, kdjw={kdjw}, kdjwavg={kdjwavg}".format(id=stockData.id, macddiff=stockData.curMacdDiffWeek,kdjw=stockData.curKDJWeek,kdjwavg=stockData.kdjWeekAvg))
          log.info("buyReason: {buyReason}, buyMsg = {buyMsg}".format(buyReason = buyReason, buyMsg = buyMsg))
          self.tradeProcess.changeSubType(gParam.PROCESS_SUB_BUY)
      if self.tradeProcess.subTradeType == gParam.PROCESS_SUB_BUY:
        # 如果已经有买单，等待买单完成，切换到buy_done状态
        # 如果没有买单，下单购买
        self.updateOrBuyOrder(context, data)
      if self.tradeProcess.subTradeType == gParam.PROCESS_SUB_BUY_DONE:
        # 如果低于周线kdj均线，切换到卖出状态
        self.cashLeft = 0
        self.updateStockCount(context)
        if self.stockCount == 0:
          log.info("trade in gParam.PROCESS_SUB_BUY_DONE, stock_id={0} but stockCount is 0".format(self.id))
          self.tradeProcess.changeSubType(gParam.PROCESS_SUB_SELL)
        else:
          if stockData.curKDJWeek == ERR_DATA:
            log.info("data error,id = {id} curKDJWeek = {curKDJ}".format(curKDJ = stockData.curKDJWeek, id = stockData.id))
            return
          sellReason = 0
          sellMsg = ""
          if stockData.curKDJWeek < stockData.kdjWeekAvg - 2.0:
            sellReason = 1
            sellMsg = "stockData.curKDJWeek < stockData.kdjWeekAvg - 1.0"
          if sellReason > 0:
            self.tradeProcess.changeSubType(gParam.PROCESS_SUB_SELL)
            log.info("change to sell, stockid={stockid}, kdjw={kdjw}, kdjwavg={kdjwavg}".format(stockid=stockData.id, kdjw=stockData.curKDJWeek, kdjwavg=stockData.kdjWeekAvg))
            log.info("sellReason: {sellReason}, Msg = {sellMsg}".format(sellReason = sellReason, sellMsg = sellMsg))
      if self.tradeProcess.subTradeType == gParam.PROCESS_SUB_SELL:
        # 如果已经有卖单，等待卖单完成，切换到none状态
        # 如果没有卖单，下单卖出
        if self.tradeOrder != None:
          ordersDic = get_orders(order_id=self.tradeOrder.id)
          if self.tradeOrder.id in ordersDic:
            cur_order = ordersDic[self.tradeOrder.id]
            if cur_order.status == OrderStatus.held:
              log.info("sell order held, stockid={0}, orderid={1}".format(self.id, self.tradeOrder.id))
              self.tradeProcess.changeSubType(gParam.PROCESS_NONE)
              self.tradeOrder = None  # 交易成功，清除交易对象
              self.cashLeft = self.cashTotal
              return
            else:
              return
          else:
            log.info("sell order not found, stockid={0}, orderid={1}".format(self.id, self.tradeOrder.id))
            return
        # 当前没订单
        self.updateStockCount(context)

        if self.stockCount == 0:
          log.info("trade in gParam.PROCESS_SUB_SELL, stockid={0}  but stockCount is 0".format(stockData.id))
          self.tradeProcess.changeSubType(gParam.PROCESS_NONE)
          self.cashLeft = self.cashTotal
          return

        if GetDayTimeStamp(context.current_dt, 0) >= self.tradeProcess.stepEnable:
          sell_count = self.stockCount
          if sell_count == 0:
            self.tradeProcess.changeSubType(gParam.PROCESS_NONE)
            self.cashLeft = self.cashTotal
            return

          cur_price = data[self.id].avg
          orderRes = order(self.id, -sell_count)
          if orderRes == None:
            log.info("error, sell order failed: orderRes is none, id={0},price={1},stockCount={2}".format(self.id, cur_price, self.stockCount))
            return

          self.tradeOrder = TradeOrder()
          self.tradeOrder.id = orderRes.order_id
          log.info("order sell: stock_id={0}, order_id={1}, price={2}".format(self.id, orderRes.order_id, cur_price))
class TradeProcess:    #交易过程
  def __init__(self):
    self.tradeType = gParam.PROCESS_NONE  # 1 买入阶段 2 买入完成  3 卖出阶段 4 卖出完成
    self.stepEnable = 0  # 可执行操作的时间戳
    self.subTradeType = gParam.PROCESS_NONE # 子状态
  def changeType(self, context, type):
    self.tradeType = type
    self.stepEnable = GetDayTimeStamp(context.current_dt, 0)
  def changeSubType(self, type):
    self.subTradeType = type
class TradeOrder:   #交易订单
  def __init__(self):
    self.id = 0
    self.status = 0 # gParam.ORDER_RESELL gParam.ORDER_REBUY
    self.stockCount = 0  # gParam.ORDER_RESELL gParam.ORDER_REBUY时，表示剩余要交易的数量

  def statuString(self):
    if self.status == gParam.ORDER_REBUY:
        return 'REBUY'
    elif self.status == gParam.ORDER_RESELL:
        return 'RESELL'
    return 'None'
def inner_initialize():
  gParam = TraderParam()
  # 定义一个全局变量, 保存要操作的股票
  securities = gParam.securities
  g.securities = [normalize_code(x) for x in securities]
  set_universe(g.securities)

# 初始化函数，设定要操作的股票、基准等等
def initialize(context):
  inner_initialize()
  # log.info(g.securities)
  # 设定沪深300作为基准
  set_benchmark('000300.XSHG')
  # 开启动态复权(真实价格)模式
  set_option('use_real_price', True)
# 每个单位时间(如果按天回测,则每天调用一次,如果按分钟,则每分钟调用一次)调用一次
def handle_data(context, data):
  g.tradeManager.run(context, data)

# 每天 9：00 执行，更新指标数据
def before_trading_start(context):
    # 初始化 rsi 和 kdj 数据
    timeRecord = TimeRecord()
    calcKDJ = CalcKDJ()
    calcMACD = CalcMACD()
    # 初始化全局参数
    g.MAX_ROOM = gParam.ROOM_MAX
    try:
        g.tradeManager.cashTotal = context.subportfolios[0].total_value
        g.tradeManager.cashLeft = context.subportfolios[0].available_cash
        g.tradeManager.cashFree = context.subportfolios[0].available_cash - g.tradeManager.getRoomLockCash()
        log.info("cashtotal = {0}, available_cash = {1}, cashFree = {2}, rooms = {3}".format(g.tradeManager.cashTotal, g.tradeManager.cashLeft, g.tradeManager.cashFree, len(g.tradeManager.rooms)))
        # todo 前一天没完成的订单，删除订单信息
    except:
        g.tradeManager = TradeManager()
        g.tradeManager.cashTotal = context.subportfolios[0].total_value
        g.tradeManager.cashLeft = context.subportfolios[0].available_cash
        g.tradeManager.cashFree = context.subportfolios[0].available_cash
        log.info("tradeManager is None, create a new one")
        # todo: 恢复room数据
        for stockId, position in context.subportfolios[0].positions.items():
            newRoom = TradeRoom()
            newRoom.id = stockId
            newRoom.cashTotal = 0
            newRoom.cashLeft = 0
            newRoom.stockCount = position.total_amount
            newRoom.tradeProcess = TradeProcess()
            if gParam.MULTI_STATUS_MACHINE:
              newRoom.tradeProcess.tradeType = gParam.PROCESS_BUY
              newRoom.tradeProcess.subTradeType = gParam.PROCESS_SUB_BUY_DONE
            else:
              newRoom.tradeProcess.tradeType = gParam.PROCESS_BUY_DONE
            newRoom.tradeProcess.stepEnable = 0
            g.tradeManager.rooms.append(newRoom)
            log.info("restore stock position: id={0}, stockcount={1}".format(stockId, newRoom.stockCount))
    log.info("len(tradeManager.rooms) = {0}".format(len(g.tradeManager.rooms)))
    log.info("daytime: {0}".format(context.current_dt.strftime("%Y-%m-%d")))
    g.tradeManager.runCount = 0 # 重置一下每日执行次数
    preGStockDatas = g.stockDatas if hasattr(g, "stockDatas") else None # 缓存一下前一天的数据，仅在测试环境使用
    g.stockDatas = dict.fromkeys(g.securities)
    for stock in g.securities:
        # 这里取k线柱状图的数据，有一个问题：
        # 由于每次的end_date都是取数据当天，导致每次取到的数据，这里取k线柱状图
        # 的边界都是不一样的（当天往前进行切割）。
        # 这样前后两天执行时的rsi和kdj不一致。可能出现前一天不满足买入卖出条件，但是
        # 第二天一开盘就又满足了条件
        # 这里注意end_date需要传入前一天日期
        pre_date = (context.current_dt - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        klineList = None
        rowIndexList = None # 行索引是时间戳
        if gParam.PRODUCT:
          klineList = get_price(
            security=stock,
            count=gParam.KLINE_LENGTH * 30, # 这里是天数
            #end_date=datetime.datetime.now().strftime("%Y-%m-%d"),
            end_date=pre_date,
            frequency=gParam.KLINE_FREQUENCY,
            fields=['open', 'close', 'high', 'low'],
            skip_paused=True
          )
          # 生产环境的时间戳是当日8点！！ long -> datetime.date类型
          rowIndexList = [datetime.datetime.fromtimestamp(x / 1000000000).date()  for x in klineList._stat_axis.values.tolist()]
        else:
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

        #print klineList._stat_axis.values.tolist()  # 取行名称
        # columns.values.tolist()   # 取列名称
        publishDays = getStockPublishDay(rowIndexList, klineList)
        if publishDays < gParam.MIN_PUBLISH_DAYS:
          stockData = StockData()
          stockData.id = stock
          stockData.publishDays = publishDays
          g.stockDatas[stock] = stockData
          continue
        stockData = calcStockData(stock, rowIndexList, klineList, -1)
        stockData.publishDays = publishDays
        if gParam.PRODUCT:
          stockData.kdjMonthAvg = getKDJMonthAvg(rowIndexList, klineList, gParam.KDJ_MONTH_AVG_COUNT)
          stockData.kdjWeekAvg = getKDJWeekAvg(rowIndexList, klineList, gParam.KDJ_WEEK_AVG_COUNT)
        else:
          # 测试环境使用缓存的数据计算平均值, todo: 这里没有缓存上
          preGStockData = preGStockDatas[stock] if preGStockDatas and stock in preGStockDatas else None
          if preGStockData == None or preGStockData.kdjMonthAvg <= float(0.001):
            # 这里取逆序
            for preDayIndex in range(max(gParam.KDJ_MONTH_AVG_COUNT, gParam.KDJ_WEEK_AVG_COUNT)-1, -1, -1):
              preDayStockData = initStockKlineBar(stock, rowIndexList, klineList, -2 - preDayIndex)
              # 月
              monthIdx = gParam.KDJ_MONTH_AVG_COUNT - preDayIndex - 1
              if monthIdx >= 0 and monthIdx < len(stockData.kdjMonthAvgList):
                preDayStockData.preKDJMonths[0] = calcKDJ.GetKDJ(preDayStockData.kLineMonths, gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
                stockData.kdjMonthAvgList[monthIdx] = preDayStockData.preKDJMonths[0]
              # 周
              weekIdx = gParam.KDJ_WEEK_AVG_COUNT - preDayIndex - 1
              if weekIdx >= 0 and weekIdx < len(stockData.kdjWeekAvgList):
                preDayStockData.preKDJWeeks[0] = calcKDJ.GetKDJ(preDayStockData.kLineWeeks, gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
                stockData.kdjWeekAvgList[weekIdx] = preDayStockData.preKDJWeeks[0]
            stockData.kdjMonthAvg = sum(stockData.kdjMonthAvgList[:gParam.KDJ_MONTH_AVG_COUNT]) / gParam.KDJ_MONTH_AVG_COUNT
            stockData.kdjMonthAvgList.append(stockData.preKDJMonths[0]) # 把昨日的也缓存起来，省一次计算
            stockData.kdjWeekAvg = sum(stockData.kdjWeekAvgList[:gParam.KDJ_WEEK_AVG_COUNT]) / gParam.KDJ_WEEK_AVG_COUNT
            stockData.kdjWeekAvgList.append(stockData.preKDJWeeks[0]) # 把昨日的也缓存起来，省一次计算
          else:
            stockData.kdjMonthAvgList = preGStockData.kdjMonthAvgList[1:]
            stockData.kdjMonthAvgList.append(stockData.preKDJMonths[0])
            stockData.kdjMonthAvg = sum(stockData.kdjMonthAvgList[:gParam.KDJ_MONTH_AVG_COUNT]) / gParam.KDJ_MONTH_AVG_COUNT
            stockData.kdjWeekAvgList = preGStockData.kdjWeekAvgList[1:]
            stockData.kdjWeekAvgList.append(stockData.preKDJWeeks[0])
            stockData.kdjWeekAvg = sum(stockData.kdjWeekAvgList[:gParam.KDJ_WEEK_AVG_COUNT]) / gParam.KDJ_WEEK_AVG_COUNT
        preDayOpen = klineList['open'][len(rowIndexList) - 1] if len(rowIndexList) > 0 else 0
        preDayClose = klineList['close'][len(rowIndexList) - 1] if len(rowIndexList) > 0 else 0
        log.info("id = {id}, pub={publishDays}, pre_open={preDayOpen}, pre_close={preDayClose} \n pre_macd_diff_m={pre_macd_diff_m}, macd_diff_m={macd_diff_m}, pre_macd_diff_w={pre_macd_diff_w}, macd_diff_w={macd_diff_w}, k-4={pre_kdj4},k-3={pre_kdj3}, k-2={pre_kdj2}, k-1={pre_kdj1}, kdj_m = {kdj_m}, kdj_day_mavg = {kdj_day_mavg}, kdj_w = {kdj_w}, kdj_day_wavg = {kdj_day_wavg},kdj_d = {kdj_d}"\
        .format(id = stockData.id, publishDays = stockData.publishDays, \
        preDayOpen = preDayOpen, preDayClose = preDayClose, pre_macd_diff_w=stockData.preMacdDiffWeeks[1], macd_diff_w=stockData.preMacdDiffWeeks[0],\
        pre_macd_diff_m=stockData.preMacdDiffMonths[1], macd_diff_m=stockData.preMacdDiffMonths[0], kdj_m = stockData.preKDJMonths[0], kdj_day_mavg = stockData.kdjMonthAvg, pre_kdj4 = stockData.preKDJMonths[4], pre_kdj3 = stockData.preKDJMonths[3], \
        pre_kdj2 = stockData.preKDJMonths[2], pre_kdj1 = stockData.preKDJMonths[1], kdj_w = stockData.preKDJWeeks[0], kdj_day_wavg = stockData.kdjWeekAvg,kdj_d = stockData.preKDJDays[0]))

        g.stockDatas[stock] = stockData

# 获取KDJ前X日的当月平均, 从前天开始
def getKDJMonthAvg(rowIndexList, klineList, count):
  if count == 0:
    return 0
  sumVal = 0.00
  for idx in range(count):
    stockData = calcStockData(0, rowIndexList, klineList, -2 - idx)
    sumVal += stockData.preKDJMonths[0]
  return sumVal / float(count)

def getKDJWeekAvg(rowIndexList, klineList, count):
  if count == 0:
    return 0
  sumVal = 0.00
  for idx in range(count):
    stockData = calcStockData(0, rowIndexList, klineList, -2 - idx)
    sumVal += stockData.preKDJWeeks[0]
  return sumVal / float(count)

# start = -1代表昨日， -2 为前天， 以此类推， 返回的是start当天的数据
def getStockPublishDay(rowIndexList, klineList):
  if len(rowIndexList) == 0:
    return 0
  for idx in range(len(rowIndexList)-1, -1, -1):
    if numpy.isnan(klineList['open'][idx]) or len(rowIndexList)-1-idx >= gParam.MIN_PUBLISH_DAYS:
      break
  return len(rowIndexList)-1-idx

def calcStockData(stockId, rowIndexList, klineList, start):
  calcKDJ = CalcKDJ()
  calcMACD = CalcMACD()
  stockData = initStockKlineBar(stockId, rowIndexList, klineList, start)
  # Month
  for n in range(gParam.KDJ_PRE_MONTH_COUNT):
    stockData.preKDJMonths[n] = calcKDJ.GetKDJ(stockData.kLineMonths[n:], gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
  for n in range(gParam.MACD_PRE_MONTH_COUNT):
    stockData.preMacdDiffMonths[n] = calcMACD.GetDiff(stockData.kLineMonths[n:])
  stockData.curKDJMonth = stockData.preKDJMonths[0]
  stockData.curMacdDiffMonth = stockData.preMacdDiffMonths[0]
  # week
  for n in range(gParam.KDJ_PRE_WEEK_COUNT):
    stockData.preKDJWeeks[n] = calcKDJ.GetKDJ(stockData.kLineWeeks[n:], gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
  for n in range(gParam.MACD_PRE_WEEK_COUNT):
    stockData.preMacdDiffWeeks[n] = calcMACD.GetDiff(stockData.kLineWeeks[n:])
  stockData.curKDJWeek = stockData.preKDJWeeks[0]
  stockData.curMacdDiffWeek = stockData.preMacdDiffWeeks[0]
  # day
  for n in range(gParam.KDJ_PRE_DAY_COUNT):
    stockData.preKDJDays[n] = calcKDJ.GetKDJ(stockData.kLineDays[n:], gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)[1]
  stockData.curKDJDay = stockData.preKDJDays[0]
  return stockData

def initStockKlineBar(stockId, rowIndexList, klineList, start):
  stockData = StockData()
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
      # 跨月，而且上月有数据，结算上一个k线图数据
      if kLineMonth.day >= gParam.KLINE_BAR_MONTH_DAY:
        stockData.kLineMonths.append(kLineMonth)
        kLineMonth = None
      # 跨周，而且上周有数据
      if kLineWeek and kLineWeek.day >= gParam.KLINE_BAR_WEEK_DAY:
        stockData.kLineWeeks.append(kLineWeek)
        kLineWeek = None
      # 每天都是跨天
      kLineDay = None
      if kLineMonth == None:
          kLineMonth = KLineBar()
      if kLineWeek == None and len(stockData.kLineWeeks) < gParam.KLINE_LENGTH:
          kLineWeek = KLineBar()
      if kLineDay == None and len(stockData.kLineDays) < gParam.KLINE_LENGTH:
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

  if len(stockData.kLineMonths) < gParam.KLINE_LENGTH and len(stockData.kLineMonths) > 0:
      lastKline = stockData.kLineMonths[-1]
      for i in range(gParam.KLINE_LENGTH - len(stockData.kLineMonths)):
          kLineMonth = KLineBar()
          kLineMonth.open = lastKline.open
          kLineMonth.close = lastKline.close
          kLineMonth.high = lastKline.high
          kLineMonth.low = lastKline.low
          stockData.kLineMonths.append(kLineMonth)
  return stockData

def after_trading_end(context):
  # 更新持股数
  # 当天没完成的订单，根据订单状态，回滚数据
  for room in g.tradeManager.rooms:
    if room.id in context.subportfolios[0].positions:
      position = context.subportfolios[0].positions[room.id]
      room.stockCount = position.total_amount

    if room.tradeOrder != None:
      ordersDic = get_orders(order_id=room.tradeOrder.id)
      if room.tradeOrder.id in ordersDic:
        cur_order = ordersDic[room.tradeOrder.id]
        if cur_order.is_buy:
          room.cashLeft -= cur_order.filled * cur_order.price
          if room.cashLeft < 0:
            room.cashLeft = 0
        if cur_order.amount == cur_order.filled:
          # 全部成交，进入下一个阶段或者状态
          if cur_order.is_buy:
            room.cashLeft = 0
            if gParam.MULTI_STATUS_MACHINE:
              room.tradeProcess.changeSubType(gParam.PROCESS_SUB_BUY_DONE)
            else:
              room.tradeProcess.changeType(context, gParam.PROCESS_BUY_DONE)
          else:
            if gParam.MULTI_STATUS_MACHINE:
              room.tradeProcess.changeSubType(gParam.PROCESS_NONE)
              room.cashLeft = room.cashTotal
            else:
              room.tradeProcess.changeType(context, gParam.PROCESS_SELL_DONE)
          room.tradeOrder = None
        else:
          # 有未成交的情况，下一个交易日继续交易
          if cur_order.is_buy:
            room.tradeOrder.status = gParam.ORDER_REBUY
          else:
            room.tradeOrder.status = gParam.ORDER_RESELL
          room.tradeOrder.stockCount = cur_order.amount - cur_order.filled
      else:
          log.info("order not found after trading end, stockid={0}, orderid={1}".format(room.id, room.tradeOrder.id))

def after_code_changed(context):
  log.info('after_code_changed run')
  inner_initialize()
  g.tradeManager = None # 销毁交易类，下次启动时重建

