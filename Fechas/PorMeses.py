import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

data = pd.read_csv('steam.csv')

dates = data['release_date']

dicc = {}

for i in range(dates.size):
    dicc[dates[i][0]+dates[i][1]+dates[i][2]+dates[i][3]+dates[i][4]+dates[i][5]+dates[i][6]] = 0

for i in range(dates.size):
    dicc[dates[i][0]+dates[i][1]+dates[i][2]+dates[i][3]+dates[i][4]+dates[i][5]+dates[i][6]] += 1


x = np.arange(179)
fig, ax = plt.subplots(figsize=(20,10))
plt.bar(x, dicc.values())
plt.xticks(x, dicc.keys())
plt.title("Relación salida - mes de publicación")
plt.show()


