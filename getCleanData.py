import os
import numpy as np
import pandas as pd
import csv
import time,math
from joblib import Parallel, delayed

#spilt_ann = []

'''
git test
123

test02

'''

def replace(df1,df2):
    df = df1.copy()
    for rows in range(0,len(df)):
        imgname= df1.iloc[rows]['ImageID']
        foldandname= df2[df2.imageName==imgname]
        ttt = foldandname.iloc[0,1]
        df.iloc[rows,0]=ttt
    return df

def func(data1,data2):
	for j in range(72):
		df = replace(data1[j],data2)

		savePath = './cleandata_v2_' + str(j) + '.csv'
		export_csv = df.to_csv(savePath,index = None, header=True)


def concatFunc():
    image_info_v2 = "./train_v2.csv"
    dfTrainv2= pd.read_csv(os.path.basename(image_info_v2))#,nrows=25) #header=None,
    return dfTrainv2

def getCleanData(spiltNum=72,thread=6):
	
	ann_file = "./cleandata.csv"
	ann_data = pd.read_csv(os.path.basename(ann_file))#,nrows=10)
	
	spilt_ann = np.split(ann_data,spiltNum)
	print('Finish 1.')

	dfTrainv2 = concatFunc()
	print('Finish 2.')
	
	'''
	# image_info_file = "./train.csv"
	# image_info_data = pd.read_csv(os.path.basename(image_info_file),header=None)#,nrows=10)
	# image_info_data.columns = ['folder','imageName']
	
	# concat = image_info_data['folder'].str.cat(image_info_data['imageName'],sep='/')
	# image_info_data['Concat']=concat
	# print('Finish 2.')
	
	# tempDict = {}
	# tempDict = image_info_data.set_index('imageName').T.to_dict('records')
	# tempDict = tempDict[0]
    
	Don't use dictionary{} to replace data, time-consuming. 
	Using dataframe to replace is more efficient and effective.
	'''
	
	print('Ready to parallel.')
	Parallel(n_jobs=6)(delayed(func)(spilt_ann,dfTrainv2) for j in range(72))

	
if __name__ == '__main__':
    getCleanData()







