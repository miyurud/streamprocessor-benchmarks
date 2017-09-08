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



plt.ylabel("TPS(events/seconds)");
plt.xlabel("Record Id");


plt.plot(time,throughput)




plt.show()


