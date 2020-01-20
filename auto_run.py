# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 21:31:57 2019

@author: yhossam
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 15:27:00 2019

@author: yhossam
"""


import pandas as pd
import numpy as np
from BulkPDP.Arps_Fitting import *


def secantToNominal(x,b):
    return (np.power(1-x,-b)-1)/b/12

def nominalToSecant(x,b):
    return 1-np.power(12*x*b+1,-1/b)


def calcRemainingProd(qi,di,b,startMonth,ecoLimit=1):
   vals=hyp2exp_q(np.arange(startMonth,1000),qi=qi,di=di,b=b)
   return np.sum(vals[vals>ecoLimit])



# In[]


import pyodbc

drivername = "SQL Server"
dbname = "IHS"
servername = "RAISASQL1"
username = "appuser"
pwd = "appuser"

connection = pyodbc.connect("Driver={"+drivername+"};"+
                    "Server="+servername+";"+
                    "Database="+dbname+";"+
                    "uid="+username+";pwd="+pwd)




wellsForecasts=pd.read_csv("wellsForecasts.csv",dtype={"API":str})

sql = '''SELECT cast(T2.UWI AS varchar(50)) as API,T2.CompletionDate,T2.WellName,T2.BasinName,
T2.LatWGS84, T2.LonWGS84
FROM Wells AS T2 INNER JOIN
ProdnWells AS T3 ON T2.UWI = T3.API AND T3.API IN {} INNER JOIN
ProdnHeaders AS T4 ON T4._Id = T3._HeaderId INNER JOIN
ProdnAbstracts AS T6 ON T6._HeaderId = T3._HeaderId INNER JOIN
WellLocations AS T7 ON T7._WellId = T2._Id'''.format(tuple(wellsForecasts["API"]))

data = pd.read_sql(sql, connection)


data=data[["API","WellName"]]

wells=pd.merge(data,wellsForecasts,on="API").drop_duplicates("API")

# In[]


from BulkPDP.DataProcessing import *
from enum import Enum

class Resource(Enum):
    #updated variable names to lowercase (nabil)
    oil = 'Liquid'
    gas = 'Gas'
    water = 'Water'


apis=data["API"]   
 
apis=list(apis)
apis=[str(api) for api in apis]
apis=["0"+api if api.startswith("5") else str(api) for api in apis]


curtisForecasts=pd.read_excel("Data Project.xlsx")
curtisForecasts["Start"]=pd.to_datetime(curtisForecasts['Start'])
curtisForecasts["API"]=[int("0"+str(api)+"0000") for api in curtisForecasts.API10]


#get raw data 
rawProductionData = get_production_data_for_APIList_from_IHS(list(apis))
rawProductionData=pd.merge(rawProductionData,curtisForecasts,on="API")
rawProductionData['date'] = rawProductionData['Year'].astype(str) + '/' + rawProductionData['Month'].astype(str) + '/' + '1'
rawProductionData['date'] = pd.to_datetime(rawProductionData['date'])
rawProductionData=rawProductionData[rawProductionData["date"]<rawProductionData["Start"]]


wellCums=rawProductionData[["API","Liquid"]].groupby("API").sum().reset_index()
wellCums["oilTotal"]=wellCums["Liquid"]/30
wellCums=wellCums.drop(columns="Liquid")



productionData,_=process_raw_IHS_data(rawProductionData, True,Resource['oil'].value,1000,0)
productionData=pd.merge(productionData,wellCums,on="API")
monthCols=["month_"+str(i) for i in range(1,list(productionData.columns).index("API")+1)]
wells["API"]=wells["API"].astype(np.int64)
finalData=pd.merge(productionData,wells,on="API")



#
#curtisEurs=pd.read_csv("EURs.csv").rename(columns={"CASENAME":"WellName"})
#
#combined=pd.merge(curtisEurs,finalData,on="WellName")



# In[]

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import matplotlib.patches as mpatches

plt.ioff()

auto_run_log={}

curtisForecasts["API"]=["0"+str(api)+"0000" for api in curtisForecasts.API10]
curtisForecasts["di_oil"]=[secantToNominal(di/100,b) for di,b in zip(curtisForecasts["Di"],curtisForecasts["B"])]


compareFrame=pd.merge(wellsForecasts,curtisForecasts,on="API",suffixes=("_model","_curtis"))
compareFrame["API"]=compareFrame["API"].astype(np.int64)


exportFrame=pd.merge(compareFrame,productionData[["API","month_1"]],on="API").rename(columns={"month_1":"peakProduction"}).loc[:,"API10":]
exportFrame["QiToPeakRatio"]=exportFrame["Qi"]/exportFrame["peakProduction"]
print(exportFrame["QiToPeakRatio"].mean())
exportFrame.to_excel("curtisForecastsWithPeak.xlsx")

# In[]

sideCount=10
ratio=sideCount/10



plottingData=pd.merge(compareFrame,productionData,on="API")
plottingData["peakDate_oil"]=pd.to_datetime(plottingData['peakDate_oil'])
plottingData["monthShift"]=np.round((plottingData["Start"]-plottingData["peakDate_oil"])/ np.timedelta64(1,'M'))




mlEurs=[]
curtisEurs=[]
for _,row in plottingData.iterrows():
    
    mlEurs.append(calcRemainingProd(row["qi_oil"],row["di_oil_model"],row["b_oil"]
    ,row["monthsCount"],ecoLimit=3)+row["oilTotal"])
    curtisEurs.append(calcRemainingProd(row["Qi"],row["di_oil_curtis"],row["b_oil"]
    ,0,ecoLimit=3)+row["oilTotal"])
    

plottingData["mlEur"]=mlEurs


plottingData["curtisEur"]=curtisEurs
plottingData["relError"]=(plottingData["mlEur"]-plottingData["curtisEur"])/plottingData["curtisEur"]*100


plottingData["mlEur"]*=30/1000
plottingData["curtisEur"]*=30/1000



auto_run_log["curtisTotal"]=plottingData["curtisEur"].sum()
auto_run_log["modelTotal"]=plottingData["mlEur"].sum()
auto_run_log["totalRelError"]=(plottingData["mlEur"].sum()-plottingData["curtisEur"].sum())/plottingData["curtisEur"].sum()



for i in range(2):
    title="Model & Curtis Forecasts {}".format(i+1)
    fig = plt.figure(num=None, figsize=(80*ratio,60*ratio), dpi=160, facecolor='w', edgecolor='w')
    fig.subplots_adjust(hspace=0.4, wspace=0.2)
    fig.suptitle(title,fontsize=100*ratio)
    plti=1
    for index,row in plottingData.iloc[i*100:(i+1)*100].iterrows():
        monthsCount=row["monthsCount"]
        production=row[monthCols].iloc[:monthsCount]
        x=np.arange(monthsCount+12)+1
        shift=int(row["monthShift"])
        production=list(production)+[np.nan for i in range(x.shape[0]-production.shape[0])]
        ml=hyp2exp_q(x-1,qi=row["qi_oil"],di=row["di_oil_model"],b=row["b_oil"])
        curtis=hyp2exp_q(x-1,qi=row["Qi"],di=row["di_oil_curtis"],b=row["b_oil"])
        
        plt.subplot(sideCount,sideCount,plti)
        #plt.title(str(row["peakDate_oil"])+"--"+str(row["Start"]),fontdict={"fontweight":"bold"})
        plt.plot(x,production,marker='x')
        plt.plot(x,ml,marker='x')
        xCurtis=(x+shift)[:-shift]
        plt.plot(xCurtis,curtis[:xCurtis.shape[0]],marker='x')
        
        leg=plt.legend(["Actual","ML","Curtis"],loc=1)
        patches=[]
        for i,eur in enumerate(["mlEur","curtisEur","relError"]):
            patch=mpatches.Patch(color='white', label='{}: {}{}'.format(eur,round(row[eur],1),"%" if i==2 else ""))
            patch.set_alpha(0)
            patches.append(patch)
        
        plt.gca().add_artist(leg)
        plt.legend(handles=patches,fontsize=13.5,prop=FontProperties(weight="bold"),fancybox=True, framealpha=0.5,loc=2)
        
        plti+=1
    
    plt.savefig(title+".png")