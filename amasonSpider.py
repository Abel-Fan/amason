import requests
from fake_useragent import UserAgent
import os
import time
import xlrd,xlwt
from pyquery import PyQuery
from threading import Thread,Semaphore
import re
import json
import queue


ua = UserAgent()

with open('countrys.json', 'r', encoding='utf-8') as f:
    Country = json.load(f)

class Amason:
    def __init__(self):
        self.workbookname = "data.xlsx"
        self.data = [] # url status country 信息
        self.sem = Semaphore(5)
        self.q = ""
        self.num = ""
    def getUrls(self):
        global q
        res = os.path.isfile(self.workbookname)
        if not res:
            raise ValueError("没有此文件")
            return
        else:
            
            book = xlrd.open_workbook(self.workbookname)
            sheet = book.sheet_by_index(0)
            self.num = sheet.nrows-1
            for row in range(1,sheet.nrows):
                url = sheet.cell_value(row,1)
                status = sheet.cell_value(row,2)
                country = sheet.cell_value(row,3)
                self.data.append((url,int(status),country))
            # 全局 队列 q
            self.q = queue.Queue(sheet.nrows-1)
    
    def getProInfo(self):
        """
        获取产品信息 ： 分类 排名 品牌
        """
        for info in self.data:
            t = ProInfoSpider(*info,q=self.q)
            t.start()

    def save(self):
        t = Thread(target=self.savetoexcel)
        t.start()
        
    def savetoexcel(self):
        t = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(time.time()))
        lists = ['id','url','category','country','ranking','brom','sales']
        book = xlwt.Workbook()
        mysheel = book.add_sheet('亚马逊')
        for i in range(0,len(lists)):
            mysheel.write(0,i,lists[i])
        index=0
        for i in range(self.num):
            data = self.q.get()
            index+=1
            print(index,"获取数据",data)
            mysheel.write(index,0,index)#编号
            mysheel.write(index,1,data.get("url"))#网址
            mysheel.write(index,2,data.get("category"))#面包屑
            mysheel.write(index,3,data.get('country'))#1
            mysheel.write(index,4,data.get("ranking"))  # 美国
            mysheel.write(index,5,data.get('brom'))  # 排名
            mysheel.write(index,6,data.get('sales'))#品牌
        print("正在保存数据")
        book.save(t+'.xls')

class ProInfoSpider(Thread):
    semaphore = Semaphore(10)
    def __init__(self,url,status,country,q):
        super(ProInfoSpider,self).__init__()
        self.url = url  # 地址
        self.status = status  # 状态
        self.country = country # 国家
        self.q = q

    def run(self):
        with self.semaphore:
            dom = PyQuery(self.url,headers={
                'User-Agent':ua.chrome,
                # 'Referer':self.url,
                'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
                'accept-encoding':'gzip, deflate, br',
            })
            # 分类
            category =  " ".join(dom("#wayfinding-breadcrumbs_feature_div").text().split())
            # 卖家
            brom = dom("#bylineInfo").text()

            # 销量所需 类目
            typename = " ".join(dom('#wayfinding-breadcrumbs_feature_div>ul>li:nth-child(1)>span>a').text().split())
            
            # 排名 销量
            ranking = ""
            sales = ""
            try:
                if dom('#SalesRank'):
                    ranking = re.findall("#([\d,]+?)\s",dom("#SalesRank").text())[0].replace(",","")
                else:
                    res = requests.get(self.url,headers={
                        'User-Agent':ua.chrome
                    })
                    ranking = re.findall("#([\d,]+?)\s[\s\w]+?\(",res.text)[0].replace(",","")

                if self.status==1:
                    sales = self.run2(self.country,typename,ranking) # 参数 ：国家 类目 排名
            except:
                print("排名销量获取异常")
            
                
            obj = {}
            obj['category'] = category # 面包屑
            obj['ranking'] = ranking # 排名
            obj['sales'] = sales # 销量
            obj['brom'] = brom # 卖家
            obj['url'] = self.url # 地址
            obj['country'] = self.country  # 国家
            # 存储到queue
            self.q.put(obj)
            

    def run2(self,country,typename,ranking):
        print(country,typename,ranking )
        esselect = Country.get(country)['id']
        categoryid = Country.get(country)['options'][typename]
        # print(country,esselect,categoryid)
        # 爬取 销量预估
        res = response = requests.post('https://www.amz520.com/tool/get_bsrranksales',headers={
            'User-Agent':ua.chrome
        },data={
            'rank':ranking,
            'categoryid':categoryid,
            'esselect':esselect
        }).json()
        if res.get("ames",None)!=None:
            return res['ames']['estsalesresult']
        else:
            return None


a = Amason()
a.getUrls()
a.getProInfo()
a.save()