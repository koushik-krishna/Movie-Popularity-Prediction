import scrapy
from scrapy import signals
import pandas as pd
from pathlib import Path
import os
import time

class QuotesSpider(scrapy.Spider):
    name = "quotes"
    movie_features = list()
    count = 0

    def start_requests(self):
        tile_ids = list()
        main_urls = list()

        path = Path(os.getcwd())
        with open(str(path.parent)+"/crew.csv") as f:
            crew_ids = f.readlines()

        for id in crew_ids:    
            crew_id = id.strip('\n')
            url = 'https://www.imdb.com/name/'+str(crew_id)+'/'
            yield scrapy.Request(url=url, meta={'crewId': crew_id})
            
        self.crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)    
        

    def spider_closed(self, spider):
        movies_df = pd.DataFrame(QuotesSpider.movie_features)
        # print(str(len(QuotesSpider.movie_features))+'*'*100)
        movies_df.to_csv('crew_data.csv', index=False)

    def parse(self, response):
        data_dict = dict()
        data_dict["crewId"] = response.meta['crewId']
        # try:
        #     data_dict["Popularity"] = response.xpath("//div[div[contains(text(),'Popularity')]]//span[@class='subText']/text()").get().strip(' "()\n')
        # except:
        #     pass
        # try:
        #     data_dict["Metascore"] = response.xpath("//div[contains(@class,'metacriticScore')]//span/text()").get().strip(' "()\n')
        # except:
        #     pass
        try:
            oscar_split_string = response.xpath("//b[contains(text(), 'Oscar')]/text()").get().split()
            for i in range(len(oscar_split_string)):
                if oscar_split_string[i].strip().lower() == 'won':
                    data_dict["Oscar Wins"] = oscar_split_string[i+1].strip()
                elif oscar_split_string[i].strip().lower() == 'nominated':
                    data_dict["Oscar Nominations"] = oscar_split_string[i+2].strip()
        except:
            pass
        try:
            win_split_string = response.xpath("//span[contains(text(), 'win')]/text()").get().split()
            if win_split_string[0].strip().lower() == 'another':
                data_dict["Other Wins"] = win_split_string[1].strip()
            else:
                data_dict["Other Wins"] = win_split_string[0].strip()
        except:
            pass
        try:
            nomination_split_string = response.xpath("//span[contains(text(), 'nomination')]/text()").get().split()
            for i in range(len(nomination_split_string)):
                if 'nomination' in nomination_split_string[i].strip().lower():
                    data_dict["Other Nominations"] = nomination_split_string[i-1].strip()
        except:
            pass
        try:
            data_dict["Sound Mix"] = '@'.join(response.xpath("//div[h3[contains(text(),'Technical Specs')]]//div[h4[contains(text(), 'Sound Mix')]]//a/text()").getall())
        except:
            pass
        try:
            data_dict["Budget"] = response.xpath("//div[@id='titleDetails']//div[h4[contains(text(),'Budget')]]/text()[2]").get().strip(' $"\n').replace(',','')
        except:
            pass
        try:
            data_dict["Opening Weekend USA"] = response.xpath("//div[@id='titleDetails']//div[h4[contains(text(),'Opening')]]/text()[2]").get().strip(' $"\n').replace(',','')
        except:
            pass
        try:
            data_dict["Gross USA"] = response.xpath("//div[@id='titleDetails']//div[h4[contains(text(),'Gross USA')]]/text()[2]").get().strip(' $"\n').replace(',','')
        except:
            pass
        try:
            data_dict["Cumulative Worldwide Gross"] = response.xpath("//div[@id='titleDetails']//div[h4[contains(text(),'Cumulative Worldwide Gross')]]/text()[2]").get().strip(' $').replace(',','')
        except:
            pass
        try:
            data_dict["Color"] = response.xpath("//div[h3[contains(text(),'Technical Specs')]]//div[h4[contains(text(),'Color')]]/a/text()").get().strip()
        except:
            pass
        try:
            data_dict["AspectRatio"] = response.xpath("//div[h3[contains(text(),'Technical Specs')]]//div[h4[contains(text(),'Aspect Ratio')]]/text()[2]").get().strip()
        except:
            pass
        QuotesSpider.movie_features.append(data_dict)
        # print(str(len(QuotesSpider.movie_features))+'&'*100)
        QuotesSpider.count += 1
        print(QuotesSpider.count)
        time.sleep(0.5)
        print(data_dict)