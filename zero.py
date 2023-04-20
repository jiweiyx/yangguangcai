
import setting
import sqlite3
import os
from pytz import timezone
from time import sleep
from datetime import datetime, timedelta, time
import random
import asyncio


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

#下面建立要给tonclient全局变量与ton沟通
cfg_url = setting.tonclient_url
cfg = requests.get(cfg_url).json()
keystore_dir = '.keystore'
Path(keystore_dir).mkdir(parents=True, exist_ok=True)
client = TonlibClient(ls_index=0,config=cfg,keystore=keystore_dir)

#下面设置区块链的钱包参数
wallet_mnemonics = setting.wallet_mnemonics
wallet_mnemonics, pub_k, priv_k, wallet = Wallets.from_mnemonics(
    mnemonics=wallet_mnemonics, version=WalletVersionEnum.v3r2, workchain=0)
address_wallet = wallet.address.to_string(True, True, True)

#下面设置一个数据库链接的参数
if not os.path.exists('./data.db'):
        logging.info("数据库不存在，开始建立数据库")
        db_conn = sqlite3.connect('data.db')
        db_conn.execute('''
            create table orders
            (order_id integer primary key autoincrement,
            order_dt int,
            issue text,
            chat_id text,
            tg_name text,
            from_address text,
            paid blob,
            pay_amount int,
            pay_hash text,
            luck_num int,
            open_num int,
            win blob,
            to_address text,
            to_amount int);
        ''')
        db_conn.commit()
        #下面建立stock数据库
        db_conn.execute('''
        CREATE TABLE "stock" (
    	"id"	INTEGER NOT NULL UNIQUE,
	    "issue"	TEXT,
	    "open_or_not"	BLOB,
	    "close_value"	NUMERIC,
	    "luck_num"	INTEGER,
	    "buyer"	INTEGER,
	    "in_amount"	INTEGER,
	    "winners"	INTEGER,
	    "total_bonus"	INTEGER,
	    PRIMARY KEY("id" AUTOINCREMENT)
        )''')
        db_conn.commit()
else:
    db_conn = sqlite3.Connection("data.db")

def get_index():

    # 从rapidapi.com获得上证股票信息
    headers = {'X-RapidAPI-Key': setting.RapidAPI_Key}
    url="https://apidojo-yahoo-finance-v1.p.rapidapi.com/market/v2/get-quotes?region=US&symbols=000001.SS"
    res = requests.get(url,headers=headers)
    result = res.json()
    # regularMarketTime是以秒计算的累计数，
    # regularMarketPrice是这个时间段的价格
    regularMarketTime = result['quoteResponse']['result'][0]['regularMarketTime']
    regularMarketPrice = result['quoteResponse']['result'][0]['regularMarketPrice']
    rdt = datetime.fromtimestamp(regularMarketTime)
    return rdt, regularMarketPrice

async def get_balance(ACCOUNT) -> int:

    try:
        status = await client.raw_get_account_state(ACCOUNT)
        balance = status['balance']
        return int(balance)
    except Exception as err:
        logging.info("get_balance出错了，%s",err)


async def choose_winner(context: ContextTypes.DEFAULT_TYPE) -> None:

    current_time = datetime.now()
    logging.info("开奖函数被启动！")
    tz = timezone('Asia/Shanghai')
    current_time = current_time.replace(tzinfo=tz)
    # 将时间换成东八区，防止服务器时间不对出错
    issue = current_time.strftime("%Y%m%d")
    # 虽然此任务不会在周末运行，以防万一，再检查一遍
    if current_time.weekday() != 5 and current_time.weekday() != 6:  # 不是周末
        if current_time.hour > 10:  # 交易时间，检查今天有没有开市
            market_time, market_value = get_index()
            if current_time.day == market_time.day:  # 如果拿到的是当天的报价证明开盘了
                if current_time.hour >= 16:  # 而且现在已经下午四点以后了
                    luck_num = int(round(market_value*100 %
                                   100, 0))  # 后两位四舍五入取整
                    # 然后存入到stock表中
                    insert_query = f"insert into stock(issue,open_or_not,close_value,luck_num) values({issue},True,{market_value},{luck_num})"
                    db_conn.execute(insert_query)
                    db_conn.commit()
                    logging.info("已存入stock表格")
                    # 下面我们来开奖
                    update_query = f"update orders set open_num={luck_num} where issue='{issue}'"
                    db_conn.execute(update_query)
                    db_conn.commit()
                    logging.info("已更新中奖号码到orders表格")
                    # 然后我们来将中奖用户选出来（把win设置为true）
                    update_win = f"update orders set win=1 where luck_num=open_num and issue='{issue}'"
                    db_conn.execute(update_win)
                    db_conn.commit()
                    logging.info("已将中奖的人的win设置为true")
                    # 然后来计算中奖的人共付了多少钱
                    cal_in_amount = f"select order_id,pay_amount from orders where win is True and issue='{issue}'"
                    cur = db_conn.execute(cal_in_amount)
                    rows = cur.fetchall()
                    if len(rows) != 0:
                        total_in = 0
                        for row in rows:
                            total_in += row[1]
                        ACCOUNT = setting.ACCOUNT
                        wallet_bls = await get_balance(ACCOUNT)
                        # 先算每一个ton赢多少，防止不同的人付款不一致
                        each_coin_win = round(wallet_bls*0.9/total_in, 0)
                        for row in rows:
                            # 将分得的奖金存进去
                            write_bonus = f"update orders set to_amount={each_coin_win*row[1]} where order_id={row[0]}"
                            db_conn.execute(write_bonus)
                        db_conn.commit()
                        logging.info("奖金已经存入")

                    # 给所有购买本期彩票的人发个是否中奖的消息
                    find_buyers = f"select win,chat_id,tg_name,order_dt,issue,order_id,pay_amount,luck_num from orders where issue='{issue}'"
                    cur = db_conn.cursor
                    cur = db_conn.execute(find_buyers)
                    buyers = cur.fetchall()
                    for buyer in buyers:
                        if buyer[0]:
                            news = f"{buyer[2]},你好！ 你在{buyer[3]}购买的第{buyer[4]}期阳光彩，订单编号{buyer[5]}，你付款了{buyer[6]/1000000000}TON，选的幸运数字是{buyer[7]}。开奖了。今天上证指数闭市时点位是{market_value}，小数点后两位四舍五入后是{luck_num}，恭喜你，中奖了。请点击 /his 来查看历史购买情况和兑奖！"
                        else:
                            news = f"{buyer[2]},你好！ 你在{buyer[3]}购买的第{buyer[4]}期阳光彩，订单编号{buyer[5]}，你付款了{buyer[6]/1000000000}TON，选的幸运数字是{buyer[7]}。开奖了。今天上证指数闭市时点位是{market_value}，小数点后两位四舍五入后是{luck_num}，不好意思，你没有猜中。欢迎点击 /new 来买下一期，祝你好运！"
                        await context.bot.send_message(buyer[1], news)
                        sleep(2)  #发送一则消息以后，需要等待1秒，否则会发送失败

            else:  # 如果拿到的报价不是当天的，证明当天没开盘
                update_query = f"insert into stock(issue,open_or_not) values('{issue}',0)"
                db_conn.execute(update_query)
                db_conn.commit()
                # 然后把所有买了当天期数的改到下一天去
                if current_time.weekday() == 4:  # 如果是周五
                    next_day = current_time+timedelta(days=3)
                else:
                    next_day = current_time+timedelta(days=1)
                next_issue = next_day.strftime("%Y%m%d")
                #看一下谁买了这一期，然后告诉他一下，今天没看盘
                select_users = f"select issue,chat_id from orders where issue={issue}"
                cur = sqlite3.Cursor
                cur = db_conn.execute(select_users)
                users = cur.fetchall()
                for user in users:
                    await context.bot.send_message(user[1],f"今天中国股市没开盘，你购买的{issue}期彩票自动顺延到了下一期：{next_issue}，特此通知，祝你好运！")
                #然后把所有的期数改一下
                change_issue = f"update orders set issue='{next_issue}' where issue='{issue}'"
                db_conn.execute(change_issue)
                db_conn.commit()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_name = update.message.from_user.first_name
    logging.info('%s用户启动了start函数',tg_name)
    balance = await get_balance(setting.ACCOUNT)
    #检查一下多少金额未兑奖，这个人有多少奖金未兑换
    check_bonus = "select sum(to_amount) from orders where to_address is null"
    cur = db_conn.execute(check_bonus)
    row = cur.fetchone()
    bonus = 0
    if row[0] != None:
        bonus = int(row[0])

    await context.bot.send_message(chat_id=update.effective_chat.id, text=f"""你好,{tg_name},欢迎来到阳光彩票！\n

本程序想借助虚拟币和智能合约，建立一个简单公正的彩票应用。
大家来猜中国下一个工作日上证指数闭市时候小数点后两位数字是多少，猜对的人就拿走奖池内90%奖金,剩余10%留给下一轮。
每个工作日下午三点停止竞猜，下午四点开奖。三点以后购买的是第二天的彩票。一注1TON。\n

目前奖池余额：{balance/1000000000}TON，你可以从这个地址确认：<a href="https://testnet.tonscan.org/address/EQAd3b5PyiksK5Uizi8azpd4fw6IJ8HDrIUEcsyAVXjG0uV8">EQAd3b5PyiksK5Uizi8azpd4fw6IJ8HDrIUEcsyAVXjG0uV8</a>
中奖将至少获得{round((balance+1000000000-bonus)*0.9,0)/1000000000}TON。

查看此消息，点击 /start 
看上期开奖，点击 /last  
购买阳光彩，点击 /new   
查询并兑奖，点击 /his   


目前程序运行在Ton的Testnet上，你可以点击<a href="https://t.me/testgiver_ton_bot">这里</a>免费获得测试TON币。""",
                                   parse_mode="HTML", disable_web_page_preview=True)
    # 下面创建一个每天下午四点运行的
    current_jobs = context.job_queue.get_jobs_by_name("check_index")
    # 先检查是不是已经创建了这个job，如果没有那就创建，否则直接跳过
    if not current_jobs:
        chat_id = update.message.chat_id
        t = time(16,0,0, tzinfo=timezone("Asia/Shanghai"))
        job = context.job_queue.run_daily(choose_winner, t, days=(1,2,3,4,5), chat_id=chat_id, name="check_index")
        logging.info("%s每日任务已创建，下次运行时间: %s", tg_name,job.next_t)

    return


async def history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_name = update.message.from_user.first_name
    logging.info('%s启动了history函数',tg_name)
    # 下面检查这个用户最近最多5次的购买情况
    check_history = f"select order_id,issue,luck_num,pay_amount,open_num,win,to_amount,to_address from orders where tg_name='{tg_name}' order by order_dt desc limit 5"
    cur = db_conn.execute(check_history)
    rows = cur.fetchall()
    if len(rows) != 0:
        # 若老用户的话回顾一下以前的情况，新用户就跳过
        recent_order_msg = "你最近5次的购买记录如下：\n------------------------------------------\n"
        for row in rows:
            recent_order_msg += f"订单：{row[0]},期数：{row[1]}, 投注数字 {row[2]}, "
            if row[3] == None:  # row[3]付款金额
                recent_order_msg += "未付款，"
            else:
                recent_order_msg += f"付款{row[3]/1000000000}TON, "
            if row[4] == None:  # row[4]，开奖数字
                recent_order_msg += "尚未开奖"
            else:
                recent_order_msg += f"中奖数：{row[4]}, "
            if row[5]: 
                recent_order_msg += f"获奖金额 {row[6]/1000000000}TON, "
                if row[7] == None:  # row[7]兑奖地址
                    recent_order_msg += "未兑奖"
                else:
                    recent_order_msg += "已兑奖"
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
        
        await update.message.reply_text(recent_order_msg)
        if bonus == 0:   #如果没有奖金要发，发完消息就结束对话
            return ConversationHandler.END
        else:
            return 2
    else:   #若没有发现购买彩票记录
        await update.message.reply_text("没有查到你购买彩票的记录，点击 /new 来买一注吧。")
        return ConversationHandler.END


async def create_order(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:

    logging.info('新建订单')
    #先把这一期已经选过的数取出来
    # 创建订单到数据库
    current_date = datetime.now()
    next_issue = current_date
    # 下面推算可以买的期数
    if current_date.weekday() == 5:
        next_issue = current_date+timedelta(days=2)  # 若是周六就是下周一
    if current_date.weekday() == 6:
        next_issue = current_date+timedelta(days=1)  # 若今天周日，就是下周一
    if current_date.weekday() == 4:  # 若是周五
        if current_date.hour > 15:   # 若已经下午三点了，那只能买下周一
            next_issue = current_date + timedelta(days=3)  # 那就买下一期
    if current_date.weekday() <= 3:
        if current_date.hour >= 15:  # 若已经下午三点了，那买第二天
            next_issue = current_date + timedelta(days=1)  # 那就买下一期
    next_issue_str = next_issue.strftime("%Y%m%d")
    
    get_selected_num = f"select luck_num from orders where issue='{next_issue_str}'"
    cur = db_conn.execute(get_selected_num)
    selected_nums = cur.fetchall()
    lst = list(map(lambda x: x[0], selected_nums))
    # 随机生成一个0-99之间的数字：
    new_rdm = str(int(random.random()*100))
    while new_rdm in lst:
        new_rdm = str(int(random.random()*100))
    # 把随机数保存起来
    chat_id = update.message.chat_id
    rdm[chat_id] = new_rdm

    # 回复员工对话
    await update.message.reply_text(f"""系统从别人还没选过的数字中帮你随机挑了个幸运数字: <b>{new_rdm} </b>\n\n 若同意的话，可以点击 /ok \n\n或直接会复发其他你喜欢的数字(0-99)。\n\n 若要结束会话，请点击或输入 /end """, parse_mode="HTML", disable_web_page_preview=True)
    return 1


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
        logging.info("订单编号%s付款超时，将删除订单。",{order_id})
        delete_order = f"DELETE FROM orders where order_id={order_id}"
        db_conn.execute(delete_order)
        db_conn.commit()
        # 告诉用户由于15分钟内未收到付款，订单已经取消了
        await context.bot.send_message(chat_id, f"订单{order_id},没有在15分钟内收到付款，订单已删除，你可以点击 /new 请重新购买！")
        return

    trans = await client.get_transactions(setting.ACCOUNT,to_transaction_lt=0,limit=10)
    for tran in trans:
        if tran['in_msg']['message'] == str_msg:
            logging.info("已经收到付款")
            from_address = tran['in_msg']['source']
            pay_amount = int(tran['in_msg']['value'])
            if pay_amount <1000000000:
                await context.bot.send_message(chat_id, f"订单{order_id}已收到付款。\n付款地址：{from_address}\n付款金额：{int(pay_amount)/1000000000} TON\n 由于你付款金额不足1TON，系统即将安排原路退回，并删除此订单，若想重新购买，请点击或输入 \new")
                try:
                    raw_seqno = await client.raw_run_method(address=address_wallet, method='seqno', stack_data=[])
                    seqno = int(raw_seqno['stack'][0][1], 16)
                except Exception as err:
                    logging.info("退款操作，转账时没能获取到seqno")
                    await context.bot.send_message(chat_id,"转账时出错了，不要着急，30秒以后会重试。")
                    return 
                    # 下面开始转账
                transfer = wallet.create_transfer_message(to_addr=from_address,
                                                        amount=pay_amount,
                                                        seqno=seqno,
                                                        payload='@yangguangcai_bot'
                                                        )
                transfer_message = transfer['message'].to_boc(False)
                try:
                    await client.raw_send_message(transfer_message)
                    update_query = f"delete from orders where order_id={order_id}"
                    db_conn.execute(update_query)
                    db_conn.commit()
                    #删除订单，取消后续检查
                    job.schedule_removal()
                    await context.bot.send_message(chat_id,f"已退款，订单{order_id}已删除。")
                    return
                except Exception as err:
                    await context.bot.send_message(chat_id,"转账时出错了，不要着急，30秒以后会重试。")
                    return 
                
            pay_hash = tran['transaction_id']['hash']
            # 下面直接写入数据库
            logging.info('将付款信息写入数据库')
            update_query = f'''update orders
                set from_address='{from_address}',
                paid=True,
                pay_amount={pay_amount},
                pay_hash='{pay_hash}'
                where order_id={order_id}'''
            db_conn.execute(update_query)
            db_conn.commit()
            # 既然都付款了，就把后续检查任务取消掉
            job.schedule_removal()
            # 然后给用户发个消息，告诉他已收到款
            await context.bot.send_message(chat_id, f"订单{order_id}已收到付款。\n付款地址：{from_address}\n付款金额：{int(pay_amount)/1000000000} TON\n 记录哈希：<a href='https://testnet.tonscan.org/tx/{pay_hash}'>{pay_hash}</a>\n祝你好运！",parse_mode="HTML", disable_web_page_preview=True)


async def create_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    
    tg_name = update.message.from_user.first_name
    logging.info('%s用户启动了创建订单函数',tg_name)
    chat_id = update.message.chat_id
    msg = str(update.message.text)
    if msg == '/end':
        await update.message.reply_text("好的，已取消，若要重新开始，请点击或输入 /new ")
        return  ConversationHandler.END
    
    if msg == '/ok':
        luck_num = rdm[chat_id]
    else:
        if msg.isdigit():
            msg_int = int(msg)
            if 0 <= msg_int < 100:
                luck_num = msg_int
            else:
                await update.message.reply_text("请输入0-99之间的数字")
        else:
            logging.info("msg内容是：%s",msg)
            await update.message.reply_text(f"你的输入有误。\n 若接受随机数{rdm[chat_id]}请点击 /ok \n或者直接回复一个0-99之间的数字\n点击 /end 来结束此会话。")
            return
    # 创建订单到数据库
    current_date = datetime.now()
    next_issue = current_date
    # 下面推算可以买的期数
    if current_date.weekday() == 5:
        next_issue = current_date+timedelta(days=2)  # 若是周六就是下周一
    if current_date.weekday() == 6:
        next_issue = current_date+timedelta(days=1)  # 若今天周日，就是下周一
    if current_date.weekday() == 4:  # 若是周五
        if current_date.hour > 15:   # 若已经下午三点了，那只能买下周一
            next_issue = current_date + timedelta(days=3)  # 那就买下一期
    if current_date.weekday() <= 3:
        if current_date.hour >= 15:  # 若已经下午三点了，那买第二天
            next_issue = current_date + timedelta(days=1)  # 那就买下一期


    next_issue_str = next_issue.strftime("%Y%m%d")

    createOrder_query = '''insert into orders(order_dt,issue,chat_id,tg_name,luck_num) values(?,?,?,?,?)'''
    chat_id = update.message.chat_id
    order_dt = int(current_date.timestamp())
    cur = db_conn.cursor
    cur = db_conn.execute(createOrder_query, (order_dt,
                                              next_issue_str, chat_id, tg_name, luck_num))
    db_conn.commit()
    order_id = cur.lastrowid
    logging.info("新订单已创建%s",order_id)
    str_dt = next_issue.strftime("%Y%m%d")
    str_msg = f"{order_id}-{tg_name}-{luck_num}-{str_dt}"
    pay_link = f"ton://transfer/{setting.ACCOUNT}?amount=1000000000&text={str_msg}"
    cpt = f'''订单已创建，编号{order_id}\n请付款到<a href="{pay_link}">{setting.ACCOUNT}</a>\nMessage:{str_msg}\n幸运数字：{luck_num}\n开奖时间：{next_issue.strftime("%Y-%m-%d")} 16:00\n'''
    # 把付款信息发送给用户
    await update.message.reply_text(cpt, parse_mode=constants.ParseMode.HTML)
    
    # 下面检查是否收到款,每30秒检查一次付款，若收到了付款就发送消息
    context.job_queue.run_repeating(
        check_payment, 30, 0, 1000, data=order_id, name=str_msg, chat_id=chat_id)

    return ConversationHandler.END


async def pay_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:

    tg_name = update.message.from_user.first_name

    address_from_msg = update.message.text
    
    if address_from_msg == '/end':
        await update.message.reply_text("好的。已结束会话，可以继续点击 /his 重新开始。")
        return  ConversationHandler.END
    
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
    if bonus != 0:
        # 证明真的要转钱给他
        logging.info(f"算好了，要给这个人{bonus}")
        try:
            raw_seqno = await client.raw_run_method(address=address_wallet, method='seqno', stack_data=[])
            seqno = int(raw_seqno['stack'][0][1], 16)
        except Exception as err:
            logging.info("转账时没能获取到seqno")
            await update.message.reply_text("转账时出错了，你可以稍等后输入 /his 再试一下。")
            return ConversationHandler.END
            # 下面开始转账
        transfer = wallet.create_transfer_message(to_addr=transfer_address,
                                                amount=bonus,
                                                seqno=seqno,
                                                payload='@yangguangcai_bot'
                                                )
        transfer_message = transfer['message'].to_boc(False)
        try:
            await client.raw_send_message(transfer_message)
            update_query = f"update orders set to_address='{to_address}' where tg_name='{tg_name}' and win is True and to_address is NULL"
            db_conn.execute(update_query)
            db_conn.commit()
            await update.message.reply_text("转账应该成功了，你检查一下。")
            return ConversationHandler.END
        except Exception as err:
            logging.info("转账时出错了")
            await update.message.reply_text("转账时出错了，你可以稍等后输入 /his 再试一下。")
            return ConversationHandler.END
        # 下面开始转账操作
        # 我们来新建一个进程转账试试

    return

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

async def show_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_name = update.message.from_user.first_name
    logging.info('%s用户启动了show_last函数',tg_name)
    #这里打算显示最后一次的开奖总结
    get_last_issue = "select issue from orders where open_num is True  order by order_id desc limit 1"
    cur = db_conn.execute(get_last_issue)
    last_issue = cur.fetchone()[0]
    get_last_index = f"select close_value,luck_num from stock where issue='{last_issue}'"
    cur = db_conn.execute(get_last_index)
    last_index = cur.fetchone()
    full_index = last_index[0]
    luck_num = last_index[1]
    get_all_last = f"select tg_name, luck_num,pay_amount,pay_hash,win,to_amount,to_address from orders where issue = '{last_issue}'"
    cur = db_conn.execute(get_all_last)
    all_last = cur.fetchall()
    msg = f"第{last_issue}期开奖情况公示如下：\n----------------------------------\n当日上证指数闭市是：{full_index}，所以幸运数字是：<b>{luck_num}</b>\n-----------------------------------\n"
    buyers = 0
    winners= 0
    bonus = 0
    for last in all_last:
        msg += f"用户：{last[0]},投注{last[1]},付款{last[2]/1000000000}TON,交易凭证:<a href='https://testnet.tonscan.org/tx/{last[3]}'>{last[3]}</a>"
        if last[4] == True:
            winners += 1
            msg += f",中奖了,中奖金额{last[5]/1000000000}TON"
            bonus +=last [5]
        if last[6] != None:
            msg += f",已兑奖地址:<a href='https://testnet.tonscan.org/address/{last[6]}'>{last[6]}</a>"
        else:
            if last[4] == True:
                msg += f",未兑奖"
        msg +='\n------------------------------------\n'
        buyers +=1
    msg +=f"一共有{buyers}个用户购买，中奖人数：{winners}，一共中奖金额：{bonus/1000000000}\n"
    await update.message.reply_text(msg,parse_mode="HTML", disable_web_page_preview=True)
    return

if __name__ == "__main__":

    #初始化一个tonclient出来
    asyncio.get_event_loop().run_until_complete(tonclient_init())
    # 建立telegram的bot并设置TOKEN
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start),
                      CommandHandler(['his', 'history'], history),
                      CommandHandler('last',show_last),
                      CommandHandler('new', create_order)],
        states={
            1: [MessageHandler(filters.ALL, create_invoice)],
            2: [MessageHandler(filters.ALL, pay_bonus)],
        },
        fallbacks=[CommandHandler(["end", 'cancel'], cancel)]
    )
    application = Application.builder().token(setting.TOKEN).build()
    application.add_handler(conv_handler)
    #下面开始接受用户会话
    application.run_polling(2)
    #当结束时关闭tonclient的连接
    db_conn.close()  #关闭数据库
    asyncio.run(application.job_queue.stop()) #清空job_queue
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(client.close())
    loop.close()
