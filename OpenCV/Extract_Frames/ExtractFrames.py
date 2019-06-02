import cv2 

def FrameCapture(path): 
      
    vidObj=cv2.VideoCapture(path) #Path to video file 

    count=0 #counter variable
  
    success=1 #checks whether frames were extracted 

    while success: 
  
        # vidObj object calls read 
        # function extract frames 
        success,image=vidObj.read() 
  
        # Saves the frames with frame-count 
        cv2.imwrite("C:\\Users\\Abhishek\\Desktop\\Images\\frame%d.jpg" % count, image) 
  
        count+=1
  
FrameCapture("C:\\Users\\Abhishek\\Desktop\\trial.mov") 
