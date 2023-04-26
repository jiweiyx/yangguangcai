
import setting
import sqlite3
import os
from pytz import timezone
from time import sleep
from datetime import datetime, timedelta, time
import random
import asyncio
import re
import decimal
import math
import json

from telegram import Update, constants
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from tonsdk.utils._address import Address
from tonsdk.contract.wallet import Wallets, WalletVersionEnum
import requests
from pathlib import Path
from pytonlib import TonlibClient


import logging
logging.basicConfig(
    format='%(asctime)s - %(thread)d - %(funcName)s - %(message)s',
    level=logging.INFO
)

rdm = {}  # 用来保存帮用户生成的随机数。chat_id:98，类似这样
last_order={}  #用来存储上一个订单的时间，防止用户批量下单而不付款，导致服务器检查工作负荷过高
level = {
    1: "五等奖",
    2: "四等奖",
    3: "三等奖",
    4: "二等奖",
    5: "一等奖"
}

# 下面建立要给tonclient全局变量与ton沟通
cfg_url = setting.tonclient_url
cfg = requests.get(cfg_url).json()
keystore_dir = '.keystore'
Path(keystore_dir).mkdir(parents=True, exist_ok=True)
client = TonlibClient(ls_index=0, config=cfg, keystore=keystore_dir)

# 下面设置区块链的钱包参数
wallet_mnemonics = setting.wallet_mnemonics
wallet_mnemonics, pub_k, priv_k, wallet = Wallets.from_mnemonics(
    mnemonics=wallet_mnemonics, version=WalletVersionEnum.v4r2, workchain=0)
address_wallet = wallet.address.to_string(True, True, True)

# 下面设置一个数据库链接的参数
if not os.path.exists('./core.db'):
    logging.info("数据库不存在，开始建立数据库")
    db_conn = sqlite3.connect('core.db')
    db_conn.execute('''CREATE TABLE "orders" (
	"order_id"	INTEGER,
	"order_dt"	INTEGER,
	"issue"	TEXT,
	"chat_id"	TEXT,
	"tg_name"	TEXT,
	"luck_num"	TEXT,
	"paid"	BLOB,
	"pay_address"	TEXT,
	"pay_amount"	INTEGER,
	"pay_hash"	TEXT,
	"open_time"	INTEGER,
	"open_index"	INTEGER,
	"open_num"	TEXT,
	"win"	INTEGER,
	"to_time"	INTEGER,
	"to_address"	TEXT,
	"to_amount"	INTEGER,
	"to_hash"	TEXT,
	"to_msg"	TEXT,
	PRIMARY KEY("order_id" AUTOINCREMENT)
)''')
    db_conn.commit()
else:
    db_conn = sqlite3.Connection("core.db")

class _issue:
    def __init__(self) -> None:
        conn = sqlite3.Connection("core.db")
        dt = datetime.now()
        tz = timezone('Asia/Shanghai')
        dt = dt.replace(tzinfo=tz)
        str_dt = dt.strftime("%Y%m%d")
        sql = f"select open from stock where _date={str_dt}"
        cur = conn.execute(sql)
        open = cur.fetchone()[0]
        if int(open)==1:
            open_or_not = True
        else:
            open_or_not = False
        self.dt = dt
        self.str_dt = str_dt
        self.open = open_or_not
        cur.close()
        conn.close


    def next(self)->str:
        conn = sqlite3.Connection("core.db")
        tomorrow = self.dt
        open = False
        while not open:
            tomorrow += timedelta(days=1)
            str_tmr = tomorrow.strftime("%Y%m%d")
            sql = f"select open from stock where _date={str_tmr}"
            cur = conn.execute(sql)
            result = int(cur.fetchone()[0])
            open = True if result==1 else False
            if open:
                return str_tmr

def get_index():

    url = 'http://yunhq.sse.com.cn:32041/v1/sh1/snap/000001'
    response = requests.get(url)
    data_dict = json.loads(response.text)
    dt = str(data_dict['date'])+str(data_dict['time'])
    rdt = datetime.strptime(dt,'%Y%m%d%H%M%S')
    regularMarketPrice = str(data_dict['snap'][5])
    return rdt,regularMarketPrice


async def get_balance(ACCOUNT) -> int:

    try:
        status = await client.raw_get_account_state(ACCOUNT)
        balance = status['balance']
        return int(balance)
    except Exception as err:
        logging.info("get_balance出错了，%s", err)


async def choose_winner(context: ContextTypes.DEFAULT_TYPE) -> None:

    dt = _issue()
    current_time=dt.dt
    if dt.open:
        issue = dt.str_dt
        current = current.time.time()
        afternoon_time = time(15,30,30)
        if current > afternoon_time:  # 而且现在已经超过下午三点半了
                    # 加一个判断，如果已经开过奖了，就直接跳过
                    check_query = "select open_index from orders where issue='{issue}'"
                    cur = db_conn.execute(check_query)
                    check_index = cur.fetchall()
                    if len(check_index)>0:
                        logging.info("已开奖，但中奖函数被重复启动")
                        return
                    #下面获取上证指数数据
                    market_time, market_str = get_index()
                    if market_time.time()<afternoon_time:
                        logging.info("获得的index的时间不是三点半以后的，开奖失败")
                    market_num = decimal.Decimal(market_str)
                    open_num = market_num - (market_num // 10 * 10)
                    str_open_num = '{:.4f}'.format(open_num)
                    # 下面我们来开奖
                    update_query = f"update orders set open_time={current_time.timestamp()},open_index={market_num}, open_num='{str_open_num}' where issue='{issue}'"
                    db_conn.execute(update_query)
                    db_conn.commit()
                    #获得尚未兑换的奖金总额
                    unpaid_query = "select sum(to_amount) from orders where to_address is NULL"
                    cur=db_conn.execute(unpaid_query)
                    unpaid = int(cur.fetchone[0])
                    #先看看奖金够不够
                    balance = get_balance(setting.ACCOUNT)
                    balance = balance - unpaid
                    logging.info("已更新中奖号码到orders表格")
                    select_buyers = f"select order_id,luck_num,open_num from orders where paid is True and issue='{issue}'"
                    cur = db_conn.execute(select_buyers)
                    buyers = cur.fetchall()
                    
                    for buyer in buyers:
                        order_id = buyer[0]
                        luck_num = decimal.Decimal(buyer[1])
                        open_num = decimal.Decimal(buyer[2])
                        win = 0  # 几等奖
                        right = 0  # 小数点后猜中的个数
                        to_amount = 0
                        if math.floor(luck_num) == math.floor(open_num):  # 个位相等
                            win = 1
                            if math.floor(luck_num*10) % 10 == math.floor(open_num*10) % 10:  # 小数点后一位
                                right += 1
                            # 小数点第二位相等
                            if math.floor(luck_num*100) % 10 == math.floor(open_num*100) % 10:
                                right += 1
                                # 第三位相等
                            if math.floor(luck_num*1000) % 10 == math.floor(open_num*1000) % 10:
                                right += 1
                                # 第四位相等
                            if math.floor(luck_num*10000) % 10 == math.floor(open_num*10000) % 10:
                                right += 1
                        if win == 1:  # 若猜中个位数
                            
                            if right == 0 and balance > 5*1_000_000_000:
                                to_amount = 5*1_000_000_000

                            if right == 1 and balance > 20*10_000_000_000:
                                to_amount = 20*1_000_000_000
                                win += 1
                                balance = balance - to_amount

                            if right == 2 and balance > 100*1_000_000_000:
                                to_amount = 100*1_000_000_000
                                win += 1
                                balance = balance  - to_amount

                            if right == 3 and balance > 1500*1_000_000_000:
                                to_amount = 1500*1_000_000_000 
                                win += 1
                                balance = balance - to_amount

                            if right == 4 and balance > 5000*1_000_000_000:
                                to_amount = 50000*1_000_000_000
                                win += 1
                                
                        update_win = f"update orders set win={win},to_amount={to_amount} where order_id={order_id}"
                        db_conn.execute(update_win)

                    db_conn.commit()
                    logging.info("中奖信息存入了数据库")

                    # 给所有购买本期彩票的人发个是否中奖的消息
                    find_buyers = f"select win,chat_id,tg_name,order_dt,issue,order_id,luck_num,open_num,to_amount from orders where issue='{issue}'"
                    cur = db_conn.cursor
                    cur = db_conn.execute(find_buyers)
                    buyers = cur.fetchall()
                    for buyer in buyers:
                        win = buyer[0]
                        chat_id = buyer[1]
                        tg_name = buyer[2]
                        order_dt = datetime.fromtimestamp(
                            buyer[3], tz=timezone("Asia/Shanghai"))
                        issue = buyer[4]
                        order_id = buyer[5]
                        luck_num = buyer[6]
                        open_num = buyer[7]
                        to_amount = buyer[8]
                        if win:
                            news = f"""
{tg_name}, 恭喜你，中奖了！
订单编号：{order_id}
购买时间：{order_dt}
中奖期数：{issue}
竞猜数字：<b>{luck_num}</b>
上证闭市：{market_str}
开奖数字：<b>{open_num}</b>
中奖等级：{level[win]}
中奖金额：{to_amount/1_000_000_000}TON
要领奖，请点击 /his"""
                        else:
                            news = f"""
{buyer[2]},你好，你购买阳光彩没有中奖，祝你下次好运！
订单编号：{order_id}
购买时间：{order_dt}
彩票期数：{issue}
竞猜数字：<s>{luck_num}</s>
上证闭市：{market_str}
开奖数字：<b>{open_num}</b>
若需要继续购买下一期，请点击 /new"""
                        await context.bot.send_message(buyer[1], news, parse_mode='HTML')
                        sleep(2)  # 发送一则消息以后，需要等待1秒，否则会发送失败


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_name = update.message.from_user.first_name
    logging.info('%s用户启动了start函数', tg_name)
    try:
        balance = await get_balance(setting.ACCOUNT)
    except:
        balance = 0
        logging.info("获取钱包余额失败，默认回复0")
    msg = ""
    if balance > 5_000_000_000:
        msg = "猜中个位，奖金 5 TON。\n"
    if balance > 20_000_000_000:
        msg += "猜中个位，并且猜中一个小数位，奖金 20 TON。\n"
    if balance > 100_000_000_000:
        msg += "猜中个位，并且猜中两个小数位，奖金 100 TON。\n"
    if balance > 1_500_000_000_000:
        msg += "猜中个位，并且猜中三个小数位，奖金 1,500 TON,价值 5000 美金。\n"
    if balance > 50_000_000_000_000:
        msg += "全部都猜对，获得终极大奖，奖金 50,000 TON。价值十万美金。"
    await update.message.reply_html(text=f"""欢迎来到上证彩票！\n

基于智能合约的公正彩票应用。
竞猜上证指数个位和小数点后四位。
合约地址：
<code>{setting.ACCOUNT}</code>

当前奖池余额：{balance/1_000_000_000}TON

中奖规则类似双色球:
每注 1 TON
{msg}

每个工作日下午三点停止竞猜。
下午三点半准时开奖。
三点以后购买的是第二天的彩票。

查看此消息，点击 /start 
看上期开奖，点击 /last  
购买上证彩，点击 /new   
查询和兑奖，点击 /his   


目前程序运行在Ton的Testnet上, 有免费的测试币在<a href="https://t.me/testgiver_ton_bot">这里</a>领。 """,
                                   disable_web_page_preview=True)
    # 检查一下多少金额未兑奖，这个人有多少奖金未兑换
    check_bonus = "select sum(to_amount) from orders where to_address is null"
    cur = db_conn.execute(check_bonus)
    row = cur.fetchone()
    bonus = int(row[0])
    if row[0] != 0:
        await context.bot.sendMessage(chat_id=update.effective_chat.id, text=f"发现你有{bonus/1_000_000_000}Ton奖金还没有领取，请点击 /his 来查看和领取。")
    # 下面创建一个每天下午四点运行的
    current_jobs = context.job_queue.get_jobs_by_name("check_index")
    # 先检查是不是已经创建了这个job，如果没有那就创建，否则直接跳过
    if not current_jobs:
        chat_id = update.message.chat_id
        t = time(15,30,30, tzinfo=timezone("Asia/Shanghai"))
        job = context.job_queue.run_daily(choose_winner, t, days=(
            1, 2, 3, 4, 5), chat_id=chat_id, name="check_index")
        logging.info("%s每日任务已创建,下次运行时间: %s", tg_name, job.next_t)

    return


async def create_order(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:

    #检查订单间隔是否超过5分钟
    chat_id = update.message.chat_id
    dt = datetime.now()
    if chat_id in last_order:
        last = last_order[chat_id]
        duration = dt.timestamp()-last
        if duration <300:   #如果上一个订单到现在还不到5分钟
            await update.message.reply_text(f"为防止用户频繁下单不付款浪费资源，所以两订单时间间隔需要超过5分钟。建议你{int(300-duration)}秒以后再下单。上一个订单付款完成后将解除限制。")
            return ConversationHandler.END
    
    logging.info('新建订单')
    # 随机生成一个0.0000格式的随机数：
    new_rdm = round(random.random()*10, 4)
    str_rdm = '{:.4f}'.format(new_rdm)
    # 把随机数保存起来
    chat_id = update.message.chat_id
    rdm[chat_id] = str_rdm
    # 回复员工对话
    await update.message.reply_text(f"""
机选:       <b>{str_rdm}</b>
同意:       点击 /ok
自选:       类似0.0000格式回复此消息
结束会话:   点击 /end """, parse_mode="HTML")
    return 1


async def create_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:

    tg_name = update.message.from_user.first_name
    logging.info('%s用户启动了创建订单函数', tg_name)
    chat_id = update.message.chat_id

    msg = str(update.message.text)
    if msg == '/end':
        await update.message.reply_text("好的，已取消，若要重新开始，请点击或输入 /new ")
        return ConversationHandler.END

    if msg == '/ok':
        luck_num = rdm[chat_id]
    else:
        pattern = re.compile(r'^\d+\.\d{4}$')

        # 使用正则表达式对象进行匹配
        if pattern.match(msg):
            luck_num = msg
        else:
            await update.message.reply_text(f"""输入格式不对。应类似"0.0000"这样的样式。
若接受机选 <b>{rdm[chat_id]}</b> 请点击 /ok
若结束会话，请点击 /end""", parse_mode="HTML")
            return
    
    # 下面推算可以买的期数
    dt = _issue()
    if dt.open and dt.dt.hour<15:
            next_issue_str=dt.str_dt
    else:
        next_issue_str = dt.next()
    # 创建订单到数据库
    createOrder_query = '''insert into orders(order_dt,issue,chat_id,tg_name,luck_num) values(?,?,?,?,?)'''
    chat_id = update.message.chat_id
    order_dt = int(dt.dt.timestamp())
    cur = db_conn.cursor
    cur = db_conn.execute(createOrder_query, (order_dt,
                                              next_issue_str, chat_id, tg_name, luck_num))
    db_conn.commit()
    order_id = cur.lastrowid
    logging.info("新订单已创建%s", order_id)
    str_msg = f"{order_id}-{tg_name}-{luck_num}-{next_issue_str}"
    pay_link = f"ton://transfer/{setting.ACCOUNT}?amount=1000000000&text={str_msg}"
    cpt = f'''订单编号:  {order_id}
幸运数字：  <B>{luck_num}</B>
备注信息：  <code>{str_msg}</code>
开奖时间：  {next_issue_str} 16:00
------------------------------
请付款至以下地址
<a href="{pay_link}">{setting.ACCOUNT}</a>
付款时注意检查备注信息的准确性。
'''
    # 把付款信息发送给用户
    await update.message.reply_text(cpt, parse_mode=constants.ParseMode.HTML)

    #将创建订单的时间存起来，用来检查间隔
    n = datetime.now()
    last_order[chat_id]=n.timestamp()

    # 下面检查是否收到款,每30秒检查一次付款，若收到了付款就发送消息
    context.job_queue.run_repeating(
        check_payment, 30, 0, 1000, data=order_id, name=str_msg, chat_id=chat_id)

    return ConversationHandler.END


async def check_payment(context: ContextTypes.DEFAULT_TYPE) -> None:

    job = context.job
    order_id = job.data
    chat_id = job.chat_id
    str_msg = job.name

    # 我们先从数据库拿到订单创建时间，如果发现现在时间已经超过了14.5分钟，说明是最后一次检查，那就删除掉订单
    check_query = f"select order_dt from orders where order_id={order_id}"
    cur = db_conn.cursor
    cur = db_conn.execute(check_query)
    order_dt = cur.fetchone()
    if order_dt == None:
        return
    order_time = int(order_dt[0])
    current_time = int(datetime.now().timestamp())
    # 如果订单都超过15分钟了
    if current_time-order_time > 900:
        logging.info("订单编号%s付款超时，将删除订单。", {order_id})
        delete_order = f"DELETE FROM orders where order_id={order_id}"
        db_conn.execute(delete_order)
        db_conn.commit()
        # 告诉用户由于15分钟内未收到付款，订单已经取消了
        await context.bot.send_message(chat_id, f"订单{order_id},没有在15分钟内收到付款，订单已删除，你可以点击 /new 请重新购买！")
        return

    trans = await client.get_transactions(setting.ACCOUNT, to_transaction_lt=0, limit=10)
    for tran in trans:
        if tran['in_msg']['message'] == str_msg:
            logging.info("已经收到付款")
            from_address = tran['in_msg']['source']
            pay_amount = int(tran['in_msg']['value'])
            if pay_amount != 1000000000:
                await context.bot.send_message(chat_id, f"""已收到{order_id}订单的付款。
付款地址：
{from_address}
付款金额：{int(pay_amount)/1_000_000_000} TON
发现你的款金额不是1TON，
系统即将安排原路退回，并删除此订单。
若想重新购买，请点击或输入 \new""")
                try:
                    raw_seqno = await client.raw_run_method(address=address_wallet, method='seqno', stack_data=[])
                except Exception as err:
                    logging.info("退款操作，转账时没能获取到seqno")
                    await context.bot.send_message(chat_id, "转账时出错了，不要着急，30秒以后会重试。")
                    return
                # 下面开始转账
                seqno = int(raw_seqno['stack'][0][1], 16)
                transfer = wallet.create_transfer_message(to_addr=from_address,
                                                          amount=pay_amount,
                                                          seqno=seqno,
                                                          payload=str_msg
                                                          )
                transfer_message = transfer['message'].to_boc(False)
                try:
                    await client.raw_send_message(transfer_message)
                    update_query = f"delete from orders where order_id={order_id}"
                    db_conn.execute(update_query)
                    db_conn.commit()
                    # 删除订单，取消后续检查
                    job.schedule_removal()
                    await context.bot.send_message(chat_id, f"已退款，订单{order_id}已删除。")
                    return
                except Exception as err:
                    await context.bot.send_message(chat_id, "转账时出错了，不要着急，30秒以后会重试。")
                    return

            pay_hash = tran['transaction_id']['hash']
            # 下面直接写入数据库
            logging.info('将付款信息写入数据库')
            update_query = f'''update orders
                set pay_address='{from_address}',
                paid=True,
                pay_amount={pay_amount},
                pay_hash='{pay_hash}'
                where order_id={order_id}'''
            db_conn.execute(update_query)
            db_conn.commit()
            # 既然都付款了，就把后续检查任务取消掉
            job.schedule_removal()
            #这里把订单间隔时间也给去掉
            if chat_id in last_order:
                del last_order[chat_id]
            # 然后给用户发个消息，告诉他已收到款
            await context.bot.send_message(chat_id, f"""订单{order_id}已收到付款。
付款地址：
<a href='https://testnet.tonscan.org/address/{from_address}'>{from_address}</a>
付款金额：{int(pay_amount)/1000000000} TON
备注信息：<code>{str_msg}</code>
付款凭证：<a href='https://testnet.tonscan.org/tx/{pay_hash}'>{pay_hash}</a>
""", parse_mode="HTML", disable_web_page_preview=True)
            
            #找到一个就可以了，然后跳出循坏
            break
    
    return

async def history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_name = update.message.from_user.first_name
    logging.info('%s启动了history函数', tg_name)
    # 下面检查这个用户最近最多5次的购买情况
    check_history = f"select order_id,issue,luck_num,pay_amount,open_num,win,to_amount,to_address from orders where tg_name='{tg_name}' order by order_dt desc limit 5"
    cur = db_conn.execute(check_history)
    rows = cur.fetchall()
    if len(rows) != 0:
        # 若老用户的话回顾一下以前的情况，新用户就跳过
        recent_order_msg = "你最近5次的购买记录如下:\n------------------------------------------\n"
        for row in rows:
            recent_order_msg += f"订单：{row[0]} 期数：{row[1]}\n投注数字: <b>{row[2]}</b> "
            if row[3] == None:  # row[3]付款金额
                recent_order_msg += ", 未付款\n"
            if row[4] == None:  # row[4]，开奖数字
                recent_order_msg += ", 未开奖"
            else:
                recent_order_msg += f"开奖结果:<b>{row[4]}</b>\n"
            if row[5]:
                recent_order_msg += f"获奖金额:<b>{row[6]/1000000000}TON</b>"
                if row[7] == None:  # row[7]兑奖地址
                    recent_order_msg += ", 未兑奖"
                else:
                    recent_order_msg += ", 已兑奖"
            else:
                if row[4] != None:
                    recent_order_msg += "未中奖"
            # 最后加上一行分割线
            recent_order_msg += "\n------------------------------------------\n"

        # 下面检查共有多少金额没兑奖
        bonus = float(0)
        check_bonus = f"select sum(to_amount) from orders where tg_name='{tg_name}' and to_address is null"
        cur = db_conn.execute(check_bonus)
        rows = cur.fetchone()
        if rows[0] != None:
            bonus = float(rows[0])
        if bonus != 0:
            recent_order_msg += f"发现你有{bonus/1000000000} TON 的奖金没有领取。\n请输入兑奖地址：\n"

        await update.message.reply_text(recent_order_msg, parse_mode="HTML")
        if bonus == 0:  # 如果没有奖金要发，发完消息就结束对话
            return ConversationHandler.END
        else:
            return 2
    else:  # 若没有发现购买彩票记录
        await update.message.reply_text("没有查到你购买彩票的记录，点击 /new 来买一注吧。")
        return ConversationHandler.END


async def show_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_name = update.message.from_user.first_name
    logging.info('%s用户启动了show_last函数', tg_name)
    # 这里显示5个中奖的数据吧
    get_all_last = f"select order_id,issue,open_index,open_num,tg_name,luck_num,pay_hash,win,to_amount,to_hash from orders where win is True order by order_id desc limit 5"
    cur = db_conn.execute(get_all_last)
    all_last = cur.fetchall()
    msg = f"最近5个中奖情况公示如下：\n----------------------------------"
    for last in all_last:
        msg += f"""
订单编号: {last[0]}
中奖期数：{last[1]}
当日闭市：{last[2]} <b>[{last[3]}]</b>
中奖用户: {last[4]}
竞猜数字: {last[5]}
竞猜凭证: <a href='https://testnet.tonscan.org/tx/{last[6]}'>{last[6]}</a>
中奖等级：{level[last[7]]}
获得奖金：{last[8]/1_000_000_000}TON\n"""
        if last[9] != None:
            msg += f"兑奖凭证: <a href='https://testnet.tonscan.org/tx/{last[9]}'>{last[9]}</a>"
        else:
            msg += f"尚未兑奖"
        msg += '\n------------------------------------\n'
    await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
    return


async def pay_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:

    tg_name = update.message.from_user.first_name

    address_from_msg = update.message.text

    if address_from_msg == '/end':
        await update.message.reply_text("好的。已结束会话，可以继续点击 /his 重新开始。")
        return ConversationHandler.END

    logging.info("检查钱包的正确性")
    # 我们来检查是否地址是否有效
    try:
        transfer_address = Address(any_form=address_from_msg)
        to_address = transfer_address.to_string(True, True, True)
        logging.info(f"即将准备将奖金转入这个钱包{to_address}")
    except:
        await update.message.reply_text("你输入的地址不对,你再确认一下。\n可点击 /end 结束会话")
        return
    # 下面我们再来算一遍要给这个人多少钱
    # 下面检查共有多少金额没兑奖
    bonus = float(0)
    check_bonus = f"select sum(to_amount) from orders where tg_name='{tg_name}' and to_address is null"
    cur = db_conn.execute(check_bonus)
    rows = cur.fetchone()
    if rows[0] != None:
        bonus = float(rows[0])
    if bonus ==0:
        await update.message.reply_text("没有发现需要付款给你。请再确认一下。")
        return ConversationHandler.END
    
    # 证明真的要转钱给他
    logging.info(f"算好了，要给这个人{bonus}")
    raw_seqno = await client.generic_get_account_state(address_wallet)
    seqno = raw_seqno['account_state']['seqno']
    # 生成一个唯一ID以供鉴别
    dt =datetime.now()
    pay_msg = "yangguangcai_prize_"+dt.strftime("%Y%m%d")
    transfer = wallet.create_transfer_message(to_addr=transfer_address,
                                                amount=bonus,
                                                seqno=seqno,
                                                payload=pay_msg
                                                )
    transfer_message = transfer['message'].to_boc(False)
    try:
        await client.raw_send_message(transfer_message)
    except Exception as err:
        logging.info("转账时出错了,%s", err)
        await update.message.reply_text("转账时出错了，你可以稍等后输入 /his 再试一下。")
        return ConversationHandler.END
    
    #先把付款信息写进去，就算找不到hash也不至于出错
    update_query=f"update orders set to_address='{to_address}',to_msg='{pay_msg}' where tg_name='{tg_name}' and win is True and to_address is NULL"
    db_conn.execute(update_query)
    db_conn.commit()
    msg=f"""奖金已经发送成功！
到账地址：<code>{to_address}</code>
转账金额：{bonus/1_000_000_000}TON
鉴别号码：{pay_msg}"""

    #然后尝试找一下HASH，找不到就算了
    found = False
    for i in range(5):
        sleep(2)
        trans = await client.get_transactions(setting.ACCOUNT, to_transaction_lt=0, limit=3)
        for tran in trans:
            if len(tran['out_msgs'])>0:
                if tran['out_msgs'][0]['message'] == pay_msg:
                    found = True
                    to_hash = tran['transaction_id']['hash']
                    to_time = int(tran['utime'])
                    update_query = f"update orders set to_hash='{to_hash}',to_time={to_time} where tg_name='{tg_name}' and win is True"
                    db_conn.execute(update_query)
                    db_conn.commit()
                    msg += f"\n转账凭证:<a href='https://testnet.tonscan.org/tx/{to_hash}'>{to_hash}</a>"
                    break    
        
        #既然已经找到就不用循环了
        if found == True:
            break
    
    await update.message.reply_text(msg, parse_mode=constants.ParseMode.HTML, disable_web_page_preview=True)    
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    logging.info("会话结束")
    await update.message.reply_text(
        "好的，再见！\n")
    return ConversationHandler.END


async def tonclient_init():
    try:
        await client.init()
    except:
        logging.info("初始化tonclient失败")


if __name__ == "__main__":

    # 初始化一个tonclient出来
    asyncio.get_event_loop().run_until_complete(tonclient_init())
    # 建立telegram的bot并设置TOKEN
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start),
                      CommandHandler(['his', 'history'], history),
                      CommandHandler('last', show_last),
                      CommandHandler('new', create_order)],
        states={
            1: [MessageHandler(filters.ALL, create_invoice)],
            2: [MessageHandler(filters.ALL, pay_bonus)],
        },
        fallbacks=[CommandHandler(["end", 'cancel'], cancel)]
    )
    application = Application.builder().token(setting.TOKEN).build()
    application.add_handler(conv_handler)
    # 下面开始接受用户会话
    application.run_polling(2)
    # 当结束时关闭tonclient的连接
    db_conn.close()  # 关闭数据库
    asyncio.run(application.job_queue.stop())  # 清空job_queue
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(client.close())
    loop.close()
