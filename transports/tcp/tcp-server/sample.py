#!/usr/bin/env python3.6
# Copyright 2017 WSO2 Inc. (http://wso2.org)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# ----------------------------------------------------------------------------
# Create charts from the summary.csv file
# ----------------------------------------------------------------------------
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

sns.set_style("darkgrid")

df = pd.read_csv('tcp-client-results/output-26-1504328018793.csv')
# Filter errors
#df=df.loc[df['Error Count'] < 100]
#unique_sleep_times=df['Sleep Time (ms)'].unique()
#unique_message_sizes=df['Message Size (Bytes)'].unique()

fig, ax = plt.subplots()
fig.set_size_inches(8, 6)

def save_line_chart(chart, column, sleep_time, title):
    print("Creating " + chart + " chart for " + str(sleep_time) + "ms backend delay")
    # Make sure all charts have the same max limit and there is enough space for legend
    max_limit=(int(df[column].max() / 1000) + 2) * 1000
    sns_plot = sns.pointplot(x="Concurrent Users", y=column, hue="Message Size (Bytes)", data=df.loc[df['Sleep Time (ms)'] == sleep_time], ci=None, dodge=True)
    plt.suptitle(title)
    sns_plot.set_ylim(0, max_limit)
    plt.legend(loc=2, frameon=True, title="Message Size in Bytes")
    plt.savefig(chart + "_" + str(sleep_time) + "ms.png")
    plt.clf()



for sleep_time in unique_sleep_times:
    save_line_chart("thrpt", "Throughput", sleep_time, "Throughput (Requests/sec) vs Concurrent Users for " + str(sleep_time) + "ms backend delay")
    save_line_chart("avgt", "Average (ms)", sleep_time, "Average Response Time (ms) vs Concurrent Users for " + str(sleep_time) + "ms backend delay")
   

print("Done")
