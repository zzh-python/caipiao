import csv
import time

import requests
import re
import queue
import threading
from fake_useragent import UserAgent
from lxml import etree

#部分通用函数
class MainFunction(object):

    def __init__(self):
        self.headers={
        'User-Agent': self.get_ua()
        }
    #请求网址，返回响应
    def requests_url(self,url,type='get',encoding='utf-8',data={}):
        for i in range(2):
            try :
                if type == 'get':
                     respond = requests.get(url,headers=self.headers)
                elif type == 'post':
                    respond = requests.post(url,headers=self.headers,data=data)
                if respond.status_code ==200 :
                    respond.encoding=encoding
                    return respond
            except:
                pass
    #获取ua
    def get_ua(self):
        ua=UserAgent()
        return ua.random
    #保存文件
    def save_file_to_csv(self,path,list,mode='a',encoding='utf-8'):
        with open(path,mode,encoding=encoding,newline='') as f:
            csv_writer=csv.writer(f)
            csv_writer.writerow(list)  #按行来写,传入list数据

    #提取数据
    def extraction_data(self,type,data,rule):
        if type == 're':
            list=re.findall(rule,data)
            return list
        elif type == 'xpath':
            html = etree.HTML(data)
            list=html.xpath(rule)
            return list


#需求1比赛信息获取
class RequireOne(MainFunction,threading.Thread):
    def __init__(self,url=None,queue_of_requireone=None):
        MainFunction.__init__(self)
        threading.Thread.__init__(self)
        # super(RequireOne, self).__init__()
        self.url=url
        self.queue_of_requireone=queue_of_requireone

    #获取完场比分数据
    def run(self):
        while True:
            if self.url.empty() == False:
                url=self.url.get()
                response=self.requests_url(url,encoding='gb2312')
                self.queue_of_requireone.put(response)
            else:
                return True


#需求1解析比赛信息
class AnalyzeOne(MainFunction,threading.Thread):
    def __init__(self,response=None):
        # super().__init__()
        MainFunction.__init__(self)
        threading.Thread.__init__(self)
        self.response=response

    def run(self):
        print("准备解析")
        #解析出需求2的网址
        #解析保存需求1数据
        while FlagAnalyzeOne==True:
            try:
                response=self.response.get(block=False)
                self.get_save_data(response)
            except:
                    pass

    def get_save_data(self,response):
        print("开始解析比赛信息")
        # 解析出需求2的网址
        url_list = self.extraction_data('xpath', response.text, '//td[@align="center"]/a[3]/@href')
        for url in url_list:
            queue_of_ouzhi.put(url)
        # 解析保存需求1数据
        time = self.extraction_data('xpath', response.text, '//tbody/tr/td[3]/text()')                   #时间
        if time ==[]:
            return False #网页无信息，不继续查找
        saishi = self.extraction_data('xpath', response.text, '//tr/td[@class="ssbox_01"]/a/text()')   #赛事
        zhudui=self.extraction_data('xpath',response.text,'//span[@class="mainName"]/text()')           #主队
        kedui=self.extraction_data('xpath',response.text,'//span[@class="clientName"]/text()')          #客队
        #比分
        html = etree.HTML(response.text)
        list = html.xpath('//tbody/tr')
        #连接比分字符串
        score_list=[]
        for i in list:
            score = i.xpath('./td/div[@class="pk"]/a/text()')
            a = ''
            for i in score:
                a=a+i
            score_list.append(a)
        a=len(time)
        for i in range(a):
            all_message_list=[time[i],saishi[i],zhudui[i],kedui[i],score_list[i]]
            #暂时存储比赛信息
            save_queue1.put(all_message_list)


#需求2 让球地址和大小球地址获取
class RequireTwo(MainFunction,threading.Thread):

    def __init__(self,queue_of_ouzhi=None,analyze_two_respond=None):
        MainFunction.__init__(self)
        threading.Thread.__init__(self)
        self.url_queue=queue_of_ouzhi
        self.analyze_two_respond=analyze_two_respond

    def run(self):
        while  FlagRequireTwo:
            try:
                url=self.url_queue.get()
                print('百家欧赔地址:'+url)
                response=self.requests_url(url,encoding='gb2312')
                #响应存入
                self.analyze_two_respond.put(response)
                time.sleep(2)
            except:
                pass


#解析需求2得到的网址，即解析百家欧赔数据
class AnalyzeTwo(MainFunction,threading.Thread):

    def __init__(self,analyze_two_respond=None,queue_of_rangqiu=None,queue_of_daxiao=None):
        MainFunction.__init__(self)
        threading.Thread.__init__(self)
        self.response=analyze_two_respond
        self.rangqiu_url=queue_of_rangqiu
        self.daxiao_url=queue_of_daxiao

    def run(self):
        # 返回让球指数地址和大小指数地址
        # 得到用户数据
        while True:
            try:
                response=self.response.get()
                self.get_save_data(response)
                if FlagRequireTwo ==False and self.response.empty()==True:
                    print('线程2结束')
                    break
            except:
                pass

    def get_save_data(self,response):
        print('解析百家欧赔')
        html = etree.HTML(response.text)
        #官方博彩
        list1 = html.xpath('//*[@id="1"]/td/table/tbody')
        self.write_data(list1)
        # 立博
        list3 = html.xpath('//*[@id="2"]/td/table/tbody')
        self.write_data(list3)
        #澳门
        list2 = html.xpath('//*[@id="5"]/td/table/tbody')
        self.write_data(list2)
        # 威廉293
        list5 = html.xpath('//*[@id="293"]/td/table/tbody')
        self.write_data(list5)
        # bet365
        list4 = html.xpath('//*[@id="3"]/td/table/tbody')
        self.write_data(list4)

        # 让球指数大小球指数地址
        list = html.xpath('//ul[@class="odds_nav_list"]/li[4]/a/@href')
        rangqiu = re.sub('\.\.', '', list[0])
        rangqiu='http://odds.500.com'+rangqiu
        list = html.xpath('//ul[@class="odds_nav_list"]/li[6]/a/@href')
        daxiao =re.sub('\.\.', '', list[0])
        daxiao='http://odds.500.com'+daxiao
        #存入地址
        self.rangqiu_url.put(rangqiu)
        self.daxiao_url.put(daxiao)

    def write_data(self,list):
        if len(list) == 0 :
            #无信息空行
                save_queue2.put([])
        else:
            odds = list[0].xpath('./tr/td/text()')
            # print(odds)
            list= [odds[0],odds[1],odds[2],odds[3], odds[4], odds[5]]
            save_queue2.put(list)


#请求让球地址和大小指数地址
class RequireThree(MainFunction,threading.Thread):

    def __init__(self,queue_of_rangqiu=None,queue_of_daxiao=None,queue_of_rq_res=None,queue_of_dx_res=None):
        MainFunction.__init__(self)
        threading.Thread.__init__(self)
        #让球url队列
        self.queue_of_rangqiu=queue_of_rangqiu
        #大小指数url队列
        self.queue_of_daxiao=queue_of_daxiao
        #让球响应队列
        self.queue_of_rq_res=queue_of_rq_res
        #大小响应队列
        self.queue_of_dx_res=queue_of_dx_res

    def run(self):
        while FlagRequireThree:
            try:
                url_rangqiu=self.queue_of_rangqiu.get(block=False)
                print('让球地址：'+ url_rangqiu)
                url_daxiao = self.queue_of_daxiao.get(block=False)
                print('大小地址：' + url_daxiao)
                response1 = self.requests_url(url=url_rangqiu, encoding='gb2312')
                response2 = self.requests_url(url=url_daxiao, encoding='gb2312')
                #响应存入
                self.queue_of_rq_res.put(response1)
                self.queue_of_dx_res.put(response2)
            except:
                pass


#解析让球地址和大小指数地址
class AnalyzeThree(MainFunction,threading.Thread):

    def __init__(self,queue_of_rq_res=None,queue_of_dx_res=None):
        MainFunction.__init__(self)
        threading.Thread.__init__(self)
        self.queue_of_rq_res=queue_of_rq_res
        self.queue_of_dx_res=queue_of_dx_res

    #得到用户数据
    def run(self):
        while True:
            try:
                response_rq=self.queue_of_rq_res.get(block=False)
                response_dx=self.queue_of_dx_res.get(block=False)
                print('获取响应')
                self.get_save_data(response_rq,response_dx)
                if FlagRequireThree ==False and self.queue_of_rq_res.empty()==True and self.queue_of_dx_res.empty()==True:
                    break
            except:
                pass

    def get_save_data(self, res1, res2):
        self.rangqiu(res1)
        self.daxiao(res2)
        self.all_save()

    def rangqiu(self,res1):
        print('让球指数中找到并记录下让球竞彩官方的数值')
        #找到并记录下让球竞彩官方的数值
        list_gf_1 = self.extraction_data('xpath', res1.text,'//tbody/tr/td[2][@title="竞彩官方"]')
        if len(list_gf_1)== 0:
            print('让球指数没有竞彩官方')
            list_none=[]
            for i in range(5):
                self.write_data_rangqiu(list_none)
            return False  #这没有需要采集的数据，打印空行
        value = self.extraction_data('xpath', res1.text, '//tbody/tr/td[2][@title="竞彩官方"]/../td[3]/text()')[0]
        list_gf_2 = self.extraction_data('xpath', res1.text, '//tbody/tr/td[2][@title="竞彩官方"]/../td[3][contains(text(),"-1")]/../td[4]//td/text()')
        #立博
        list_lb_1 = self.extraction_data('xpath', res1.text, '//tbody/tr/td[2][@title="立博"]')
        if len(list_lb_1) >0:
            list_lb_2 = self.extraction_data('xpath', res1.text,f'//tbody/tr/td[2][@title="立博"]/../td[3][contains(text(),"{value}")]/../td[4]//td/text()')
        else:
            list_lb_2=[]
        #威廉希尔
        list_wlxe_1 = self.extraction_data('xpath', res1.text, '//tbody/tr/td[2][@title="威廉希尔"]')
        if len(list_wlxe_1) >0:
            list_wlxe_2 = self.extraction_data('xpath', res1.text,f'//tbody/tr/td[2][@title="威廉希尔"]/../td[3][contains(text(),"{value}")]/../td[4]//td/text()')
        else:
            list_wlxe_2=[]
        #澳门
        list_am1 = self.extraction_data('xpath', res1.text, '//tbody/tr/td[2][@title="澳门"]')
        if len(list_am1) >0:
            list_am_2 = self.extraction_data('xpath', res1.text,f'//tbody/tr/td[2][@title="澳门"]/../td[3][contains(text(),"{value}")]/../td[4]//td/text()')
        else:
            list_am_2=[]
        #Bet365
        list_Bet365_1 = self.extraction_data('xpath', res1.text, '//tbody/tr/td[2][@title="Bet365"]')
        if len(list_Bet365_1) >0:
            list_Bet365_2 = self.extraction_data('xpath', res1.text,f'//tbody/tr/td[2][@title="Bet365"]/../td[3][contains(text(),"{value}")]/../td[4]//td/text()')
        else:
            list_Bet365_2=[]
        self.write_data_rangqiu(list_gf_2)
        self.write_data_rangqiu(list_lb_2)
        self.write_data_rangqiu(list_am_2)
        self.write_data_rangqiu(list_wlxe_2)
        self.write_data_rangqiu(list_Bet365_2)


    def daxiao(self,res2):
        print('大小指数数据获取')
        libo = self.extraction_data('xpath', res2.text,
                                    '//tr/td[2][@class="tb_plgs"]/p/a[@title="立博"]/../../../td/table/tbody/tr/td/text()')
        wlxe = self.extraction_data('xpath', res2.text,
                                    '//tr/td[2][@class="tb_plgs"]/p/a[@title="澳门"]/../../../td/table/tbody/tr/td/text()')
        aomen = self.extraction_data('xpath', res2.text,
                                    '//tr/td[2][@class="tb_plgs"]/p/a[@title="威廉希尔"]/../../../td/table/tbody/tr/td/text()')
        Bet365 = self.extraction_data('xpath', res2.text,
                                    '//tr/td[2][@class="tb_plgs"]/p/a[@title="Bet365"]/../../../td/table/tbody/tr/td/text()')
        #清洗数据,并保存
        list_guanfang=[]
        self.clear_data(list_guanfang)
        self.clear_data(libo)
        self.clear_data(aomen)
        self.clear_data(wlxe)
        self.clear_data(Bet365)

    #保存让球数据
    def write_data_rangqiu(self,list):
        if len(list)== 0:
            save_queue3.put([])
        else:
            save_queue3.put(list)

    # 清洗大小球数据,并保存
    def clear_data(self,list):
        if len(list)>0:
            new_list=[list[0],list[1],list[2],list[4],list[5],list[6]]
        else:
            new_list=[]
        save_queue4.put(new_list)

    def all_save(self):
        list_none=['\t','\t','\t','\t','\t','\t']
        list1 =save_queue1.get()
        for i in range(5):
            list2 = save_queue2.get()
            list3 = save_queue3.get()
            list4 = save_queue4.get()
            if i == 0 :
                list1.append('竞彩官方')
            elif i ==1 :
                list1=['\t','\t','\t','\t','\t','立博']
            elif i ==2 :
                list1=['\t','\t','\t','\t','\t','澳门']
            elif i ==3 :
                list1=['\t','\t','\t','\t','\t','威廉希尔']
            elif i ==4 :
                list1=['\t','\t','\t','\t','\t','Bet365']

            if list2 != [] :
                 for j in list2:
                      list1.append(j)
            else:
                for t in list_none:
                     list1.append(t)

            if list3 != [] :
                 for m in list3:
                      list1.append(m)
            else:
                for t in list_none:
                     list1.append(t)

            if list4 != [] :
                 for n in list4:
                      list1.append(n)
            else:
                for t in list_none:
                     list1.append(t)

            self.save_file_to_csv("E:\\untitled\\homework_spider\\csv\\football2.csv", list1)



if __name__ == '__main__':
    #需求1的url
    queue_url = queue.Queue()
    #需求1的响应
    queue_of_requireone=queue.Queue()
    #需求2的地址
    queue_of_ouzhi=queue.Queue()
    #解析需求123停止旗帜
    FlagAnalyzeOne=True
    FlagRequireTwo=True
    FlagRequireThree=True
    #解析2需要的响应
    analyze_two_respond=queue.Queue()
    #需求2停止请求旗帜
    #需求3地址,让球指数地址和大小指数地址
    queue_of_rangqiu=queue.Queue()
    queue_of_daxiao=queue.Queue()
    #让球响应队列，大小响应队列
    queue_of_rq_res=queue.Queue()
    queue_of_dx_res=queue.Queue()

    save_queue1 = queue.Queue()
    save_queue2 = queue.Queue()
    save_queue3 = queue.Queue()
    save_queue4 = queue.Queue()
    #构造请求url
    url = 'https://live.500.com/wanchang.php?e=2016-01-01'
    for y in range(2016,2020):
        for m in range(1,13):
            if y == 2019 and m == 7:
                break
            if m < 10 :
                m= '0'+ str(m)
            for d in range(1,32):
                if d < 10:
                    d = '0' + str(d)
                url='https://live.500.com/wanchang.php?e='+str(y)+'-'+str(m)+'-'+ str(d)
                queue_url.put(url)

    # 实例化线程
    crawler_thread = RequireOne(url=queue_url,queue_of_requireone=queue_of_requireone)
    parser_thread = AnalyzeOne(response=queue_of_requireone)
    crawler_thread2=RequireTwo(queue_of_ouzhi=queue_of_ouzhi,analyze_two_respond=analyze_two_respond)
    parser_thread2=AnalyzeTwo(analyze_two_respond=analyze_two_respond,queue_of_rangqiu=queue_of_rangqiu,queue_of_daxiao=queue_of_daxiao)
    crawler_thread3=RequireThree(queue_of_rangqiu=queue_of_rangqiu,queue_of_daxiao=queue_of_daxiao,queue_of_rq_res=queue_of_rq_res,queue_of_dx_res=queue_of_dx_res)
    parser_thread3=AnalyzeThree(queue_of_rq_res=queue_of_rq_res,queue_of_dx_res=queue_of_dx_res)
    # 启动
    crawler_thread.start()
    parser_thread.start()
    crawler_thread2.start()
    parser_thread2.start()
    crawler_thread3.start()
    parser_thread3.start()

    crawler_thread.join()
    while True:
        if queue_url.empty() == True and queue_of_requireone.empty() ==True:
             FlagAnalyzeOne=False
             break
    parser_thread.join()
    print('线程1结束')
    # 用于请求的url队列为空，且前面的进程停止了
    while True:
        if queue_of_ouzhi.empty() ==True:
            FlagRequireTwo=False
            break
    crawler_thread2.join()
    parser_thread2.join()
    print('线程2结束')
    #用于请求的url队列为空，且前面的进程停止了
    while True:
        if queue_of_rangqiu.empty() == True and queue_of_daxiao.empty() ==True:
            FlagRequireThree=False
            break
    crawler_thread3.join()
    parser_thread3.join()
    print('线程3结束')
    print('主线程结束')


