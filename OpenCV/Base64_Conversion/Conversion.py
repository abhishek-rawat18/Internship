import cv2 
import base64

def FrameCapture(path): 
      
    vidObj=cv2.VideoCapture(path) #Path to video file 

    count=0 #counter variable
  
    success=1 #checks whether frames were extracted 

    while success:
        
        #vidObj object calls read 
        #function extract frames 
        success,image=vidObj.read() 

        retval,buffer=cv2.imencode('.jpg', image)
        jpg_as_text=base64.b64encode(buffer) #Conversion to base64
 
        #Conversion back to binary
        jpg_original=base64.b64decode(jpg_as_text)

        # Write to a file to show conversion worked
        with open('C:\\Users\\Abhishek\\Desktop\\Images\\frame%d.jpg' % count, 'wb') as f_output:
            f_output.write(jpg_original)
            
        count+=1
  
FrameCapture("C:\\Users\\Abhishek\\Desktop\\trial.mov") 
