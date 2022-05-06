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
    # 所需技术数据
    self.needSkills = [SKILL_KDJ]

    self.MIN_BUY_COUNT = 100       # 最小买股数

    # 交易参数
    self.PRODUCT = True # 是否是生产环境
    self.MULTI_STATUS_MACHINE = False # 是否使用多层状态机
    self.KLINE_FREQUENCY = "1d"
    self.KLINE_LENGTH = 60       # 月K线数量， 最多取 60个月数据
    self.MIN_PUBLISH_DAYS = 24 * 19 # 最少上市天数
    self.ROOM_MAX = 1 # 要交易的股票数
    self.BUY_INTERVAL_DAY = 1
    self.SELL_INTERVAL_DAY = 1
    self.SH_DEAD_KDJ_LINE = 90.00  # 上证指数kdj超过这个数值，停止交易，卖出所有持仓
    self.SH_STOP_BUY_KDJ_LINE = 87.00 # 上证指数kdj超过这个数值，停止买入
    self.ONE_STOCK_BUY_KDJ_LINE = 85.00 # 个股的kdj超过这个数值，停止买入

    self.RSI_PARAM = 5
    self.KDJ_PARAM1 = 9
    self.KDJ_PARAM2 = 3
    self.KDJ_PARAM3 = 3
    self.KDJ_PRE_MONTH_COUNT = 5 # KDJ月线缓存数
    self.KDJ_PRE_WEEK_COUNT = 5 # KDJ周线缓存数
    self.KDJ_PRE_DAY_COUNT = 60 # KDJ日线缓存数
    self.KDJ_MONTH_AVG_COUNT = 40 # KDJ每日月均线缓存数（前X天的月KDJ列表,用于计算平均值）
    self.KDJ_WEEK_AVG_COUNT = 10 # KDJ每日周均线缓存数
    self.MACD_PRE_MONTH_COUNT = 2 # MACD月线缓存数
    self.MACD_DIFF_PRE_WEEK_COUNT = 10 # MACD周线缓存数
    self.MACD_DEA_PRE_WEEK_COUNT = 5 # MACD_DEA周线缓存
    self.AVG_S_COUNT = 13 # 平均线短线天数
    self.AVG_M_COUNT = 34 # 平均线中线天数
    self.AVG_L_COUNT = 55 # 平均线长线天数
    self.AVG_PRE_DAY_COUNT = 3
    # k线参数
    self.KLINE_SPLIT_TYPE = SPLIT_KLINE_NORMAL
    self.KLINE_BAR_MONTH_DAY = 40 # k线月线FIXED的天数
    self.KLINE_BAR_WEEK_DAY = 10 # k线周线FIXED的天数
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
      # 上证指数也考虑进来
      SH_CODE,
      '601000'
    ] if self.PRODUCT else [
      SH_CODE,
      '601000'
    ]
gParam = TraderParam()

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
        recalcKDJ = True
        # if self.runCount == 1:
        #     # 离收盘差5分钟时再执行一次
        #     if context.current_dt.hour == 14 and context.current_dt.minute >= 55:
        #         recalcKDJ = True
        #     else:
        #         return
        # if self.runCount >= 2:
        #     # log.info("TradeManager daily runcount = {0}".format(self.runCount))
        #     return
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
                # 新一天，周，月增加数据单元
                if self.runCount == 1:
                  if gParam.KLINE_SPLIT_TYPE == SPLIT_KLINE_FIXED_DAY:
                    pass
                  elif gParam.KLINE_SPLIT_TYPE == SPLIT_KLINE_NORMAL:
                    preDate = stockData.getPreTradeDay()
                    kLineMonth = None
                    kLineWeek = None
                    kLineDay = KLineBar()
                    if context.current_dt.month != preDate.month:
                      kLineMonth = KLineBar()
                      kLineMonth.UpdatePreDayData(cur_price, cur_price, cur_price, cur_price)
                    if context.current_dt.isocalendar()[1] != preDate.isocalendar()[1]:
                      kLineWeek = KLineBar()
                      kLineWeek.UpdatePreDayData(cur_price, cur_price, cur_price, cur_price)
                    kLineDay.UpdatePreDayData(cur_price, cur_price, cur_price, cur_price)
                    if kLineMonth != None:
                      stockData.kLineMonths.insert(0, kLineMonth)
                    if kLineWeek != None:
                      stockData.kLineWeeks.insert(0, kLineWeek)
                    stockData.kLineDays.insert(0, kLineDay)
                  # 跨自然月
                if len(stockData.kLineMonths) > 0:
                  if cur_price > stockData.kLineMonths[0].high:
                    stockData.kLineMonths[0].high = cur_price
                  elif cur_price < stockData.kLineMonths[0].low:
                    stockData.kLineMonths[0].low = cur_price
                  stockData.kLineMonths[0].close = cur_price
                  (stockData.curKDJMonth_K, stockData.curKDJMonth) = calcKDJ.GetKDJ(stockData.kLineMonths, gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)
                  stockData.curMacdDiffMonth = calcMACD.GetDiff(stockData.kLineMonths)
                if len(stockData.kLineWeeks) > 0:
                  if cur_price > stockData.kLineWeeks[0].high:
                    stockData.kLineWeeks[0].high = cur_price
                  elif cur_price < stockData.kLineWeeks[0].low:
                    stockData.kLineWeeks[0].low = cur_price
                  stockData.kLineWeeks[0].close = cur_price
                  (stockData.curKDJWeek_K, stockData.curKDJWeek) = calcKDJ.GetKDJ(stockData.kLineWeeks, gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)
                  stockData.curMacdDiffWeek = calcMACD.GetDiff(stockData.kLineWeeks)
                if len(stockData.kLineDays) > 0:
                  if cur_price > stockData.kLineDays[0].high:
                    stockData.kLineDays[0].high = cur_price
                  elif cur_price < stockData.kLineDays[0].low:
                    stockData.kLineDays[0].low = cur_price
                  stockData.kLineDays[0].close = cur_price
                  (stockData.curKDJDay_K, stockData.curKDJDay) = calcKDJ.GetKDJ(stockData.kLineDays, gParam.KDJ_PARAM1, gParam.KDJ_PARAM2, gParam.KDJ_PARAM3)
                #if self.runCount == 1 or (context.current_dt.hour == 14 and context.current_dt.minute >= 45):
                #  log.info("price={price}, kdjDay_K={kdjDay_K}, kdjDay={kdjDay}, preKdjDay={preKdjDay}, preKdjDay1={preKdjDay1}".format(price=cur_price, kdjDay_K=stockData.curKDJDay_K, kdjDay=stockData.curKDJDay, preKdjDay=stockData.preKDJDays[0], preKdjDay1=stockData.preKDJDays[1]))
        if self.runCount == 1:
          log.info("shData: price={price}, kdjMonth={kdjMonth}".format(price=shData.kLineDays[0].close, kdjMonth=shData.curKDJMonth))
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

                # 1. 上市天数大于24个月
                # 2. 如果kdj D值的月线上升，而且diff月线也上升，判定为可买入
                # 3. 如果周线上涨，在日线底部反转点买入
                if stockData.curKDJMonth < gParam.ONE_STOCK_BUY_KDJ_LINE:
                  buyReason = 0
                  buyMsg = ""
                  # if monthDiff1 > 0 and monthDiff2 < 0 and monthDiff3 < 0 and monthMacdDiff > 0:
                  #   buyReason = 1
                  #   buyMsg = "monthDiff1 > 0 and monthDiff2 < 0 and monthDiff3 < 0 and monthMacdDiff > 0 and weekDiff1 > 0 and weekMacdDiff > 0"
                  # log.info("judge price={price}, kdjDay_K={kdjDay_K}, kdjDay={kdjDay}, preKdjDay={preKdjDay}, preKdjDay1={preKdjDay1}".format(price=cur_price, kdjDay_K=stockData.curKDJDay_K, kdjDay=stockData.curKDJDay, preKdjDay=stockData.preKDJDays[0], preKdjDay1=stockData.preKDJDays[1]))
                  maxKdjValue = 35.0000
                  kdjDayCrest = stockData.getLastKdjDayCrest(maxKdjValue)
                  if stockData.curKDJDay < maxKdjValue and stockData.curKDJMonth > stockData.preKDJMonths[0] and \
                  stockData.curKDJDay > stockData.preKDJDays[0] + 0.5 and \
                  stockData.preKDJDays[0] < stockData.preKDJDays[1] and \
                  kdjDayCrest - stockData.curKDJDay >= 30.000:
                    buyReason = 2
                    buyMsg = "stockData.curKDJDay < 33.0000 and stockData.curKDJDay > stockData.preKDJDays[0] + 0.5 and stockData.preKDJDays[0] < stockData.preKDJDays[1]"
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
                    log.info("enter room, stockid={stockid}, preKDJ_1={preKDJ_1}, preKDJ_0={preKDJ_0}, curKDJ={curKDJ}, lockCash={lockCash}".format(stockid = stockData.id, preKDJ_1 = stockData.preKDJDays[1], preKDJ_0 = stockData.preKDJDays[0], curKDJ = stockData.curKDJDay, lockCash = roomCash))
                    log.info("buyReason: {buyReason}, buyMsg = {buyMsg}".format(buyReason = buyReason, buyMsg = buyMsg))
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
      if stockData.curKDJMonth < stockData.kdjMonthAvg - 4.50 or stockData.serialNegetiveMACDWeekDiff(7):
        sellReason = 1
        sellMsg = "stockData.curKDJMonth < stockData.kdjMonthAvg - 4.50"
      if sellReason > 0:
        self.tradeProcess.changeType(context, gParam.PROCESS_SELL)
        log.info("change to sell, stockid={stockid}, preKDJ_1={preKDJ_1}, curKDJ={curKDJ}".format(stockid = self.id, preKDJ_1 = stockData.preKDJMonths[1], curKDJ = stockData.curKDJMonth))
        log.info("sellReason: {sellReason}, Msg = {sellMsg}".format(sellReason = sellReason, sellMsg = sellMsg))
        return True
      return False

    def dayDecideSell(self, context):
      stockData = g.stockDatas[self.id]
      if stockData.curKDJDay == ERR_DATA:
        log.info("data error,id = {id} curKDJDay = {curKDJDay}".format(curKDJDay=stockData.curKDJDay, id=stockData.id))
        return False

      sellReason = 0
      sellMsg = ""
      # 反转，判定为卖出
      if stockData.curKDJDay_K < stockData.preKDJDays_K[0] - 2.50 or stockData.curKDJDay_K < stockData.preKDJDays_K[1] - 2.50:
        sellReason = 1
        sellMsg = "stockData.curKDJDay_K < stockData.preKDJDays_K[0] - 2.50"
      if sellReason > 0:
        self.tradeProcess.changeType(context, gParam.PROCESS_SELL)
        log.info("change to sell, stockid={stockid}, preKDJDays_K={preKDJDays_K}, curKDJDay_K={curKDJDay_K}".format(stockid = self.id, preKDJDays_K = stockData.preKDJDays_K[0], curKDJDay_K = stockData.curKDJDay_K))
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
                    #第二天才可以卖出
                    self.tradeProcess.stepEnable = GetDayTimeStamp(context.current_dt, 1)
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
      self.dayDecideSell(context)

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
    #self.stepEnable = GetDayTimeStamp(context.current_dt, 0)
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
    # timeRecord = TimeRecord()
    dataFactory = DataFactory(gParam)
    dataFactory.openLog = False

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
    #preGStockDatas = g.stockDatas if hasattr(g, "stockDatas") else None # 缓存一下前一天的数据，仅在测试环境使用
    g.stockDatas = dataFactory.genAllStockData(g.securities, context.current_dt)

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

