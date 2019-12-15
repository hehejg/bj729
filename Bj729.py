import requests
import asyncio
import aiohttp
import os
from bj729.db import motor_helper, mongo_helper
from aiostream import stream
import nest_asyncio
from types import AsyncGeneratorType
import motor.motor_asyncio

sema = asyncio.Semaphore(10)


class Bj729(object):
    def __init__(self):

        # client = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)
        # db = client['meitu']
        # self.collection = db['img_url']
        self.start_url = ""
        self.dic = {}
        self.path = r'E:\美图下载'
        self.list = []
        self.data = []
        nest_asyncio.apply()

    async def get_buff(self, url, sem, session):

        # session = aiohttp.ClientSession(connector=self.tc)
        async with session.get(url,) as response:
            if response.status == 200:
                buff = await response.read()
                # if len(buff):
                return buff

    def get_datas(self, page):

        url = f"https://bj729.com/api/?d=pc&c=picture&m=lists&timestamp=1575462993763&page={page}&sort_type="
        response = requests.get(url)
        result = response.json()
        if len(result['data']['data']) == 0: return True
        print(result['data']['data'])
        for data in result['data']['data']:
            dic = {}
            title = data['title']
            main_pic = data['main_pic']
            dic['id'] = data['id']
            dic["title"] = title
            dic['main_pic'] = main_pic
            dic['status'] = 0
            self.list.append(dic)
        mongo_helper.Mongo().save_data(self.list)
        print('保存完毕')
        return False

    async def save_pictures(self, item, session):
        extension = item["main_pic"].split('.')[-1]
        async with sema:
            buff = await self.get_buff(item['main_pic'], sema, session)
            with open(os.path.join(self.path, f"{item['title']}.{extension}", ), 'ab') as f:
                f.write(buff)
                print(item['title'], '下载完成')
                await motor_helper.MotorBase().change_status(item['id'], 1)

    # async def do_find(self):
    #     self.data = []
    #     cursor = self.collection.find({"status": 0})
    #     for document in await cursor.to_list(length=30):
    #         # print(document)
    #         self.data.append(document)

    # async def change_status(self, id, status_code=0):
    #     # status_code 0:初始,1:开始下载，2下载完了
    #     # storage.info(f"修改状态,此时的数据是:{item}")
    #     item = {}
    #     item["status"] = status_code
    #     await self.collection.update_one({'id': id}, {'$set': item}, upsert=True)

    async def branch(self, coros, limit=10):
        '''
        使用aiostream模块对异步生成器做一个切片操作。这里并发量为10.
        :param coros: 异步生成器
        :param limit: 并发次数
        :return:
        '''
        index = 0
        while True:
            xs = stream.preserve(coros)
            ys = xs[index:index + limit]
            t = await stream.list(ys)
            if not t:
                break
            await asyncio.ensure_future(asyncio.wait(t))
            index += limit + 1

    async def run(self):
        motor_helper.MotorBase().reset_all_status()
        data: AsyncGeneratorType = await motor_helper.MotorBase().find()
        async with aiohttp.connector.TCPConnector(limit=50, force_close=True, enable_cleanup_closed=True) as tc:
            async with aiohttp.ClientSession(connector=tc,) as session:
                tasks = (asyncio.ensure_future(self.save_pictures(item, session)) async for item in data)
                await self.branch(tasks)

        #     await self.do_find()
        #     if len(self.data) == 0:
        #         break
        #     # tasks = [asyncio.ensure_future(request()) for _ in range(5)]
        #     tasks = [asyncio.ensure_future(self.save_picture(item)) for item in self.data]
        #     # [await t for t in task]
        #     loop.run_until_complete(asyncio.wait(tasks))
    def seed_run(self):
        page=0
        while 1:
            page+=1
            result=self.get_datas(page)
            if result:
                break


if __name__ == '__main__':
    B = Bj729()
    asyncio.run(B.run())
