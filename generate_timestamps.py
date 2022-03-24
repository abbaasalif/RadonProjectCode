import datetime

test_date = datetime.datetime(1997, 1, 4)
res=[]
for i in range(24):
    test_date = test_date+datetime.timedelta(hours=1)
    res.append(test_date.time().strftime("%H:%M"))

with open('time.csv','w') as f:
    for i in res:
        f.write(i)
        f.write("\n")