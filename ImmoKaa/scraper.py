from bs4 import BeautifulSoup
import urllib.request as urllib2
import random
from random import choice
import pandas as pd
import copy, time, sys, shutil, os, yaml, json
import datetime as dt
from glob import glob
import regex

class scraper():
    
    criteria = None
    df = None
    df_pre = None
    __verbose = False
    __parameter_names = { #this dict translate the parameters into thei corresponding url bit
        'min_price' : 'pf',
        'max_price' : 'pt',
        'min_rooms' : 'nrf',
        'max_rooms' : 'nrt',
        'radius'    : 'r',
        'days_old'  : 'pa',
    }
    __instance_name = None
    __root_dir = "./ImmoKaa_data/"
    __base_dir = None
    
    
    
    def __init__(self, instance_name, criteria_file):
        self.__instance_name = instance_name
        self.__base_dir = self.__root_dir+instance_name
        os.makedirs(self.__base_dir, exist_ok=True)
        with open(criteria_file) as file:
            self.criteria = yaml.load(file, Loader=yaml.FullLoader)  
        self.get_preexisting_data()
   


    def _urlquery(self, url, verbose=False):
    # function cycles randomly through different user agents and time intervals to simulate more natural queries
        try:
            sleeptime = float(random.randint(1,6))/5
            time.sleep(sleeptime)

            agents = ['Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1309.0 Safari/537.17',
            'Mozilla/5.0 (compatible; MSIE 10.6; Windows NT 6.1; Trident/5.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727) 3gpp-gba UNTRUSTED/1.0',
            'Opera/12.80 (Windows NT 5.1; U; en) Presto/2.10.289 Version/12.02',
            'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)',
            'Mozilla/3.0',
            'Mozilla/5.0 (iPhone; U; CPU like Mac OS X; en) AppleWebKit/420+ (KHTML, like Gecko) Version/3.0 Mobile/1A543a Safari/419.3',
            'Mozilla/5.0 (Linux; U; Android 0.5; en-us) AppleWebKit/522+ (KHTML, like Gecko) Safari/419.3',
            'Opera/9.00 (Windows NT 5.1; U; en)']

            agent = choice(agents)
            opener = urllib2.build_opener()
            opener.addheaders = [('User-agent', agent)]

            html = opener.open(url).read()
            time.sleep(sleeptime)

            return html

        except Exception as e:
            if verbose: print('Something went wrong with Crawling:\n%s' % e)
            return None
        
        
        
    def _immoscout24parser(self, url, verbose=False):
        '''
        Read search results from Immoscout24.ch, given a specific url indicating the search criteria and the page number.
        '''
        if verbose: print ("Scanning the following url:", url)

        try:
            soup = BeautifulSoup(self._urlquery(url, verbose), 'html.parser')
            scripts = soup.findAll('script')
            scripts = filter(None, [script.string for script in scripts])
            sr = next(script for script in scripts if 'searchResult' in script)
            #Come cleaning... with not-so-clean code. Because ImmoScout keeps changing stuff and I can't be bothered to fix this properly every time.
            s = sr.replace(":undefined", ':"undefined"').lstrip("__INITIAL_STATE__=")
            s = regex.sub('\{"render".*?(?:\{(?:(?R)|[^{}])*})\}', '""', s)
            poss = [m.start() for m in regex.finditer('e=>', s)]
            res = s[:poss[0]]
            for i in range(len(poss)):
                end = len(s)
                if i+1 < len(poss):
                    end = poss[i+1]
                dd = regex.sub('(?:\{(?:(?R)|[^{}])*})', '""', s[poss[i]+3:end], 1)
                res += dd
            
            js = json.loads(res)
            return js
        
        except Exception as e:
            if verbose: print("Error in immoscout24 parser: %s" % e)
            return None
        
        
        
    def _make_url(self, criteria, page):
        url = 'https://www.immoscout24.ch/en/real-estate/{mode}/city-{city}?'.format(**criteria)
        for key in [x for x in criteria.keys() if x not in ['city', 'mode']]:
            try:
                url+=self.__parameter_names[key]+'='+str(criteria[key])+"&"
            except KeyError:
                raise Exception("Error in make_url", "Unsupported search parameter!")
        url = url[:-1]+"&pn="+str(page) #add page number

        return url
    
    

    def _get_listings(self, criteria, verbose):
        """
        Pull a list of listings for given criteria and cities, and put them in a dataframe.
        """
        print ("city:",criteria['city'])
        page = 0
        data_pages = []
        numberOfPages = 1
        while page<numberOfPages:
            page+=1
            url = self._make_url(criteria, page)
            resultlist_json = None
            N_attempts = 0
            while resultlist_json is None and N_attempts<5:
                try: 
                    N_attempts+=1
                    resultlist_json = self._immoscout24parser(url, verbose)
                    numberOfPages = int(resultlist_json["pages"]["searchResult"]["resultData"]["pagingData"]["totalPages"])
                    print("\tpage: {0}/{1}".format(page,numberOfPages), end=" ")
                    data = resultlist_json["pages"]["searchResult"]["resultData"]["listData"]
                    data = pd.DataFrame.from_dict(data)
                    data["searched-city"]=criteria['city'] #store which city we searched, for reference
                    data["fetch-date"]=dt.datetime.now().date()
                    print("({0} results)".format(data.shape[0]))
                    data_pages.append(copy.copy(data))
                except Exception as e:
                    print (e)
                    pass
        data_all = pd.concat(data_pages)

        return data_all
    
    
    
    def scrape(self):
        dfs = []
        for city in self.criteria['cities']:
            criteria_city = copy.copy(self.criteria)
            criteria_city['city'] = city
            del criteria_city['cities']
            dfs.append(self._get_listings(criteria_city, verbose=self.__verbose))

        self.df = pd.concat(dfs)
        
    
    
    def set_verbose(self, flag):
        if not isinstance(flag, bool):
            raise Exception("ImmoKaa - set_verbose", "Argument must be bool.")
        self.__verbose=flag
        
        
        
    def save_scraped_dataframe(self):
        if self.df is None:
            raise Exception("There is no scraped dataset to save.")
        today = dt.datetime.now().date().strftime("%Y-%m-%d")
        self.df.to_csv(self.__base_dir+"/serach_results_"+today+".csv", mode="w")
        print ("History file created/overwritten.")
        
        
        
    def get_preexisting_data(self):
        pres = []
        try:
            for f in glob(self.__base_dir+"/serach_results_*.csv"):
                pres.append(pd.read_csv(f))
                pres[-1]["fetch-date"] = pd.to_datetime(pres[-1]['fetch-date'],\
                                                            format="%Y-%m-%d").dt.date
            self.df_pre = pd.concat(pres)
            print ("Found {0} pre-existing data file(s). You can access the full dataset using get_full_dataset().". format(len(pres)))
        except FileNotFoundError:
            pass        
        
        
    def get_full_dataset(self):
        return pd.concat([self.df, self.df_pre])