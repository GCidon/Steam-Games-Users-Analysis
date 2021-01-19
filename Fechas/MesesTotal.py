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

x = np.arange(12)
fig, ax = plt.subplots(figsize=(20,10))
plt.bar(x, num)
plt.xticks(x, ("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"))
plt.title("Relación salida - mes de publicación")
plt.show()
#plt.savefig("MesesTotal.png")


