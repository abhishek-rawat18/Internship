from kafka import KafkaProducer
import cv2 
import base64
from time import sleep

def FrameCapture(path): 
      
    vidObj=cv2.VideoCapture(path) #Path to video file 

    count=0 #counter variable
  
    success=1 #checks whether frames were extracted 

    while success:
        
        success,image=vidObj.read() 

        retval,buffer=cv2.imencode('.jpg', image)
        
        jpg_as_text=base64.b64encode(buffer) #Conversion to base64

        producer.send("test",key=count,value=jpg_as_text)

        sleep(5)
            
        count+=1
  

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda k:str(k).encode())
#key_serializer converts the keys to bytes

FrameCapture("C:\\Users\\Abhishek\\Desktop\\trial.mov")
