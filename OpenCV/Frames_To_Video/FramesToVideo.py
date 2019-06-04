#Program to convert frames back to video

import cv2
import numpy as np
import os
import re
 
img_array = []

files=[f for f in os.listdir('C:\\Users\\Abhishek\\Desktop\\Images\\') if re.match(r'.*\.jpg', f)]

#for sorting the file names properly
files.sort(key = lambda x: int(x[5:-4]))

for filename in files:

    file_loc="C:\\Users\\Abhishek\\Desktop\\Images\\"+filename
    img = cv2.imread(file_loc)
    height, width, layers = img.shape
    size = (width,height)
    img_array.append(img)
 
 
out = cv2.VideoWriter('result.mov',cv2.VideoWriter_fourcc(*'DIVX'),30,size)
#30 is the fps
#Original fps can be obtained from the other program while converting video to frames
#result.mov is the file that we'll obtain from the frames

for i in range(len(img_array)):
    out.write(img_array[i])
    
out.release()
