import cv2

cap=cv2.VideoCapture(0)
print(f"width: {cap.get(3)}, height: {cap.get(4)}")
cap.set(3,1280)
cap.set(4,960)
while(True):
    ret, frame=cap.read()

    if(ret):
        cv2.imshow('frame', frame)
        if cv2.waitKey(1) & 0xFF==ord('q'):
            break

cap.release()
cv2.destroyAllWindows()
