# Under GPL-3.0 License

import random
import asyncio
import os
import sqlite3
import re
import shutil

from datetime import datetime,timedelta
from hoshino import R,util,priv
from hoshino import Service
from hoshino.typing import CQEvent
from hoshino.util import concat_pic, pic2b64,fig2b64, FreqLimiter, DailyNumberLimiter
from .setudao import SetuDao,SetuLikeLogDao


sv = Service('setutax', help_='''一起实现群色图自由吧！
'''.strip(),enable_on_default=False)

_flmt = FreqLimiter(10)

GO_CQHTTP_PATH = 'D:/BOT/Go/'
RE_CQIMG = r"\[CQ:image,file=(?P<file>[^,\[\]]*?),url=(?P<url>[^,\[\]]*?)\]"
TAX_TIMEOUT = 60

class TaxMaster:
    def __init__(self):
        self.taxing = {}

    def is_taxing(self, gid ,uid):
        return f'g{gid}u{uid}' in self.taxing

    def start_tax(self, gid ,uid):
        return Tax(gid, uid, self)

    def get_tax(self, gid,uid):
        return self.taxing[f'g{gid}u{uid}'] if f'g{gid}u{uid}' in self.taxing else None

class Tax:
    def __init__(self, gid,uid,tax_master):
        self.gid = gid
        self.uid = uid
        self.tm = tax_master
        self.timeout = datetime.now()+timedelta(seconds=TAX_TIMEOUT)
        self.ct = 0

    def __enter__(self):
        self.tm.taxing[f'g{self.gid}u{self.uid}'] = self
        return self

    def __exit__(self, type_, value, trace):
        del self.tm.taxing[f'g{self.gid}u{self.uid}']

    def record(self,filename):
        dao = SetuDao()
        dao.add(self.gid,self.uid,filename)

tm = TaxMaster()

@sv.on_fullmatch(('缴色图税','交色图税'))
async def start_tax(bot, ev: CQEvent):
    if tm.is_taxing(ev.group_id,ev.user_id):
        await bot.finish(ev, "已经开启了上传通道，请直接发送图片，在此期间内无法执行其他BOT命令")
    with tm.start_tax(ev.group_id,ev.user_id) as tax:
        await bot.send(ev, f"色图上传通道已开启，请直接发送图片到群。不要发送动图、超过3mb的图、违规图。如果是合并转发的消息，请保证消息只有一层嵌套。{TAX_TIMEOUT}秒内没收到新图片会自动关闭通道。\n一起实现群色图自由吧！")
        #循环判断是否结束，20秒判定一次，最多5分钟也就是15次后也会关闭
        ct = 0
        while datetime.now() < tax.timeout and ct<15:
            await asyncio.sleep(20)
            ct+=1
    await bot.send(ev, f"上传通道已关闭，共收到图片：{tax.ct}张")


@sv.on_message()
async def on_rcv_setu(bot, ev: CQEvent):
    tax = tm.get_tax(ev.group_id,ev.user_id)
    if not tax:
        return
    content = str(ev.message)
    path = R.img(f'setutax/{ev.group_id}/').path
    if not os.path.exists(path):
        os.makedirs(R.img(f'setutax/{ev.group_id}/').path)
    #判断是不是合并转发
    fwid = ''
    for m in ev.message:
        if m["type"] =="forward":
            fwid=m["data"]["id"]
            break
    
    if fwid !='':
        mm = await bot.get_forward_msg(message_id=fwid)
        ct = 0
        err = 0
        if 'messages' in mm:
            for msg in mm["messages"]:
                content = msg["content"]
                match = re.findall(RE_CQIMG,content)
                if match:
                    for m in match:
                        try:
                            cachefile = await bot.get_image(file=m[0])
                            if type(cachefile) == dict:
                                fname = cachefile["file"]
                                fname = fname[fname.rfind('/')+1:]
                                size = cachefile["size"]
                                if size <= 3145728:
                                    shutil.move(GO_CQHTTP_PATH+cachefile["file"], path)
                                    ct += 1
                                    tax.record(fname)
                                else:
                                    #文件过大，删掉缓存
                                    largepic+=1
                            else:
                                err += 1
                        except:
                            err += 0
            tax.ct +=ct
            await bot.send(ev,f"收到合并转发的图片，下载成功{ct}张，下载失败{err}张")
    else:
        ct = 0
        err = 0
        largepic = 0
        match = re.findall(RE_CQIMG,content)
        if match:
            for m in match:
                try:
                    cachefile = await bot.get_image(file=m[0])
                    if type(cachefile) == dict:
                        fname = cachefile["file"]
                        fname = fname[fname.rfind('/')+1:]
                        size = cachefile["size"]
                        if size <= 3145728:
                            shutil.move(GO_CQHTTP_PATH+cachefile["file"], path)
                            ct += 1
                            tax.record(fname)
                        else:
                            #文件过大，删掉缓存
                            largepic+=1
                    else:
                        err += 1
                except:
                    err += 1
            tax.ct +=ct
            tax.timeout = datetime.now()+timedelta(seconds=TAX_TIMEOUT)
            str2 = '' if largepic==0 else f'，{largepic}张超大图片未保存'
            await bot.send(ev, f"收到图片：{ct}张{str2}")




@sv.on_command('groupsetu',aliases=('群色图'),deny_tip='服务未开启', only_to_me=False)
async def groupsetu(session):
    uid = session.ctx['user_id']
    gid = session.ctx['group_id']
    qq = session.current_arg
    aa = qq.find('[CQ:at,qq=')
    if aa<0:
        qq=0
    else:
        qq = int(qq[aa+10:qq.find(']',aa+10)])

    if not _flmt.check(uid):
        await session.send('您冲得太快了，请稍候再冲', at_sender=True)
        return
    _flmt.start_cd(uid)
    dao = SetuDao()
    setu = dao.get_setu(gid=gid,uid=qq)
    if setu:
        s = random.choice(setu)
        dao.count_increase(s['id'])
        fname = s['fname']
        msg = []
        msg.append(f"ID：{s['id']}")
        msg.append(f"上传人：{s['uid']}")
        msg.append(f"赞：{s['lk']}，踩：{s['unlk']}")
        msg.append(f"{R.img(f'setutax/{gid}/{fname}').cqcode}")
        msg.append(f"不喜欢或者违规的图请使用命令【删图 id】来删除")
        await session.send('\n'.join(msg))
    else:
        await session.send(f"色图库内没有本群的色图记录", at_sender=True)

@sv.on_prefix(('r18', '标记r18'))
async def set_tag(bot, ev: CQEvent):
    msg = ev.message.extract_plain_text()
    
    if not re.match(r"^[0-9]*$", msg):
        bot.finish(ev, '格式为：r18空格色图id，例如r18 12', at_sender=True)
        return
    setuid = int(msg)
    dao = SetuDao()
    r = dao.add_tags(setuid,'r18',ev.group_id)
    if r:
        await bot.send(ev, f'标记成功', at_sender=True)
    else:
        await bot.send(ev, f'标记失败，色图id不正确', at_sender=True)


@sv.on_prefix(('删图', '删色图'))
async def delete_setu(bot, ev: CQEvent):
    msg = ev.message.extract_plain_text()
    if not re.match(r"^[0-9]*$", msg):
        bot.finish(ev, '格式为：删图空格色图id，例如删图 12', at_sender=True)
        return
    setuid = int(msg)
    dao = SetuDao()
    r = dao.delete(ev.group_id,setuid)
    if r:
        await bot.send(ev, f'已删除', at_sender=True)
    else:
        await bot.send(ev, f'删除失败，色图id不正确', at_sender=True)

@sv.on_prefix(('点赞', '好图'))
async def setu_likes(bot, ev: CQEvent):
    msg = ev.message.extract_plain_text()
    msg = re.sub(r'[?？，,_]', ' ', msg)
    if msg=='':
        await bot.finish(ev, '格式为：点赞空格色图id，例如点赞 12', at_sender=True)
        return
    if not re.match(r"^[0-9]*$", msg):
        await bot.finish(ev, '格式为：点赞空格色图id，例如点赞 12', at_sender=True)
        return
    setuid = int(msg)
    #判断是否赞过
    sldao = SetuLikeLogDao()
    if sldao.get_user_log(ev.group_id,ev.user_id,setuid,True):
        await bot.finish(ev, '你已经赞过这张色图了', at_sender=True)
        return

    dao = SetuDao()
    r = dao.add_like(setuid,True,ev.group_id)
    if r:
        sldao.add(ev.group_id,ev.user_id,setuid,True)
        await bot.send(ev, f'点赞成功', at_sender=True)
    else:
        await bot.send(ev, f'点赞失败，色图id不正确', at_sender=True)



@sv.on_prefix(('取消点赞', '取消好图'))
async def cancel_setu_likes(bot, ev: CQEvent):
    msg = ev.message.extract_plain_text()
    msg = re.sub(r'[?？，,_]', ' ', msg)
    if msg=='':
        await bot.finish(ev, '格式为：取消点赞空格色图id，例如取消点赞 12', at_sender=True)
        return
    if not re.match(r"^[0-9]*$", msg):
        await bot.finish(ev, '格式为：取消点赞空格色图id，例如取消点赞 12', at_sender=True)
        return
    setuid = int(msg)
    sldao = SetuLikeLogDao()
    if not sldao.get_user_log(ev.group_id,ev.user_id,setuid,True):
        await bot.finish(ev, '你没有赞过这张色图', at_sender=True)
        return
    dao = SetuDao()
    r = dao.cancel_like(setuid,True,ev.group_id)
    if r:
        sldao.delete(ev.group_id,ev.user_id,setuid,True)
        await bot.send(ev, f'取消点赞成功', at_sender=True)
    else:
        await bot.send(ev, f'取消点赞失败，色图id不正确', at_sender=True)



@sv.on_prefix(('点踩', '孬图'))
async def setu_unlikes(bot, ev: CQEvent):
    msg = ev.message.extract_plain_text()
    msg = re.sub(r'[?？，,_]', ' ', msg)
    if msg=='':
        await bot.finish(ev, '格式为：点踩空格色图id，例如点踩 12', at_sender=True)
        return
    if not re.match(r"^[0-9]*$", msg):
        await bot.finish(ev, '格式为：点踩空格色图id，例如点踩 12', at_sender=True)
        return
    setuid = int(msg)
    #判断是否踩过
    sldao = SetuLikeLogDao()
    if sldao.get_user_log(ev.group_id,ev.user_id,setuid,False):
        await bot.finish(ev, '你已经踩过这张色图了', at_sender=True)
        return
    dao = SetuDao()
    r = dao.add_like(setuid,False,ev.group_id)
    if r:
        sldao.add(ev.group_id,ev.user_id,setuid,False)
        await bot.send(ev, f'点踩成功', at_sender=True)
    else:
        await bot.send(ev, f'点踩失败，色图id不正确', at_sender=True)

@sv.on_prefix(('取消点踩', '取消孬图'))
async def cancel_setu_unlikes(bot, ev: CQEvent):
    msg = ev.message.extract_plain_text()
    msg = re.sub(r'[?？，,_]', ' ', msg)
    if msg=='':
        await bot.finish(ev, '格式为：取消点踩空格色图id，例如取消点赞 12', at_sender=True)
        return
    if not re.match(r"^[0-9]*$", msg):
        await bot.finish(ev, '格式为：取消点踩空格色图id，例如取消点赞 12', at_sender=True)
        return
    setuid = int(msg)
    sldao = SetuLikeLogDao()
    if not sldao.get_user_log(ev.group_id,ev.user_id,setuid,False):
        await bot.finish(ev, '你没有踩过这张色图', at_sender=True)
        return
    dao = SetuDao()
    r = dao.cancel_like(setuid,False,ev.group_id)
    if r:
        sldao.delete(ev.group_id,ev.user_id,setuid,False)
        await bot.send(ev, f'取消点踩成功', at_sender=True)
    else:
        await bot.send(ev, f'取消点踩失败，色图id不正确', at_sender=True)
