import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

data = pd.read_csv('steam.csv')

names = data['name']
dates = data['release_date']
num = np.zeros(12)
numY = np.zeros(23)

dicc = {}

for i in range(dates.size):
    dicc[names[i]] = (dates[i][5]+dates[i][6])
    num[int(dates[i][5]+dates[i][6])-1]+=1
    numY[int(dates[i][0]+dates[i][1]+dates[i][2]+dates[i][3])%1997]+=1

x = np.arange(23)
fig, ax = plt.subplots(figsize=(20,10))
plt.bar(x, numY)
plt.xticks(x, ("1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019",))
plt.title("Relación salida - año de publicación")
plt.show()
#plt.savefig("PorAño.png")


