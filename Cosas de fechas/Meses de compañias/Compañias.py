import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

data = pd.read_csv('steam.csv')

names = data['name']
devs = data['publisher']
dates = data['release_date']

dicc = {}
diccDates = {}

for i in range(devs.size):
    dicc[devs[i]] = []

for i in range(devs.size):
    dicc[devs[i]].append(names[i])

for i in range(names.size):
    diccDates[names[i]] = (dates[i][5]+dates[i][6])

sizes = np.zeros(12)

for i in range(devs.size):
    dev = devs[i]

    for i in range(len(dicc[dev])):
        sizes[int(diccDates[dicc[dev][i]])-1]+=1

    labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

    for i in range(12):
        if(sizes[i] == 0): 
            labels[i] = ''

    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, startangle=90)
    ax1.axis('equal')
    plt.title("Meses de salida de los juegos publicados por " + dev)

    plt.savefig("Meses de " + dev)


