import numpy as np
import matplotlib.pyplot as plt
#import plotly.plotly as py

throughput=[]
time=[]



def file_read(filepath):
	testFile=open(filepath,'r')
	#for i in range():
	#testFile.next()
	
	content=testFile.readlines()[100:200]


	for line in content:
		line=line.strip()
		data=line.split(",")[1]
		data2=line.split(",")[0]
		
		float_throughput_data = float(data)
		float_id=float(data2)
		throughput.append(float_throughput_data)
		time.append(float_id)

file_read("/home/gwthamy/Software/wso2sp-4.0.0-SNAPSHOT/samples/sample-clients/tcp-server/tcp-client-results/output-25-1504327482846.csv")		

line = plt.figure()

#np.random.seed(5)
N=len(throughput)

for x in time:
	print x

x=np.array([time])
y=np.array([throughput])

plt.ylabel("TPS(events/seconds)");
plt.xlabel("Record Id");

#plt.xticks(time)
#plt.yticks(throughput)
plt.plot(x, y, "o")


# draw vertical line from (70,100) to (70, 250)
#plt.plot([70, 70], [100, 250], 'k-', lw=2)

# draw diagonal line from (70, 90) to (90, 200)
#plt.plot([70, 90], [90, 200], 'k-')

plt.show()

#plot_url = py.plot_mpl(line, filename='mpl-docs/add-line')

