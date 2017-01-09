from mpi4py import MPI
import RPi.GPIO as GPIO
import numpy
import math
from datetime import datetime
import time
import os
comm=MPI.COMM_WORLD
rank=comm.Get_rank()
GPIO.cleanup()
#Input Parameter
dutyCycle=300# basically it is In second
tolerance=10# Defining some tolreance in seconds
inc_dec_quan=1# increment and decrement quantity for increasing and decreasing the flow rate to get threshold flow rate
fac=0.5#Factor, which will be multiplied by inc_dec_quan whenever direction of movement of horizontal line will change
timeInterval=1# What is the time intervals(s) for taking the sensor data

CycleTime=900# Cycle Time like 1 day


#Input related to tank
MaxHeight=120# Maximum Hight of tank in %

#########
CompMaxHeight=MaxHeight/1.2# Computational maximum Height, System is not utilizing full capacity of tank
BuffHight=CompMaxHeight*0.25# 25% of Computational maximum Height has been kept reserve, When water level is going below this pump will be on till 40%

##Defining global variable which will be used in fun Pump_OnOff
tank_height25=1#this is to take care of when water level is below 25% then pump will on and it will continue till 40% no matter what is consumption, now when water level is becoming lower than 40% then if consumption is high pump will be on else it will be off
tank_height100=0#This is to take care of when water level is crossing 100% then pump will be turned off. when water level come down below 90% and if consumption high then motor will turned on
##

data1=numpy.array([2,4.5,6.5])#Declaration.....Programming Demand for instantaneous send and receive
def NDFRST(FlowData,Time,dutyCycle,tolerance,inc_dec_quan,fac):#NDFR=Next day flow rate and Starting time
    ave_flowRate=sum(FlowData)/len(FlowData)#Caculating average flow rate
    max_flow=max(FlowData)
    min_flow=min(FlowData)
    error=tolerance+40# initially error is being defined greater than tolerance
    preError=error
    outPutTime=0#Initialyzing total calculated runnung time
    checkCount=0# This i will use to decrease the incFac and decFac by half, after going up and down (means after one cycle)
    swap=0#This is to make sure that while increasing ave_flowRate , it should cross the maximum flow rate
    i_quan=inc_dec_quan#
    limitLoopCount=0# This will insure that whenever ave_flowRate is being increased it should not creoss the max and min value of flow rate, and also when it will become 2 then threshold flow rate =0
    osciLoopCount=0# oscilation loop count, 
    while error>=tolerance:# if error less than tolerance then exit the loop
        for i in range(1,len(FlowData)-1):# this loop for geting total running time of motor at perticular flow rate
            preDataCheck=(FlowData[i-1]<=ave_flowRate)#Taking boolean decision whether previous flow rate is less than given flow rate
            presDataCheck=(FlowData[i]<=ave_flowRate)#Taking boolean decision whether present flow rate is less than given flow rate
            if preDataCheck != presDataCheck:#If above both decisions are not equal then note down the corresponding time
                if preDataCheck==1:#While going uphill 
                    preTime=Time[i]
                else:
                    presTime=Time[i]#While going downhill
                    outPutTime=outPutTime+presTime-preTime
            elif FlowData[i-1]==FlowData[i]:
                outPutTime=outPutTime+Time[i]-Time[i-1]# means if previous flow and current flow is equal then we are increasing outPutTime

                
        Error=outPutTime-dutyCycle#Error between actual dutycycle and calculated running time
        error=abs(Error)
        if error>=tolerance:
            outPutTime=0  #Setting back to zerro for next iteration
            if (preError/abs(preError))!= (Error/abs(Error)):#If Horizontal line has gone twice up and twice down then no need to calculate further just break it
                osciLoopCount=osciLoopCount+1
                if osciLoopCount==15:
                    break
                i_quan=i_quan*fac*(-1)
            if ave_flowRate>=max_flow or ave_flowRate <=min_flow:
                limitLoopCount=limitLoopCount+1
                if limitLoopCount==3:
                    ave_flowRate=0
                    break
                i_quan=i_quan*(-1)

            ave_flowRate=ave_flowRate+i_quan

        preError=Error            
    return [ave_flowRate,outPutTime]


def waterConsum(FlowData1,FlowData2,pumpStatus):# This function will give the total water consumed by building in one day and total water consumed from the tank during pumping
    totalWatrCons=(sum(FlowData1)-0.5*(FlowData1[0]+FlowData1[-1]))*timeInterval#Total water consumption in one day
    FlowData2=numpy.array(FlowData2)#Defining numpy array to use numpy function
    pumpStatus=numpy.array(pumpStatus)#Defining numpy array to use numpy function
    pumpOn=numpy.where(pumpStatus==1)# Finding the position where pump status==1 means switched on
    FlowData2=FlowData2[pumpOn]# Array of flow rate when pump is on
    if len(FlowData2)!=0:
        durPump=(sum(FlowData2)-0.5*(FlowData2[0]+FlowData2[-1]))*timeInterval# Total wate drawn from tank during pumping time
    else:
        durPump=0
    return [totalWatrCons,durPump]



def Pump_OnOff(ThreFlowRate,WatConsRate,Height): #This is the function to switch on or off the pump based on threshold flow rate,current water consumption rate and hight of water in the tank
    global tank_height25
    global tank_height100
    if WatConsRate>=ThreFlowRate:
        pumpStatus=1# Making desire that pump should be turned on
        PUMP=1# And pump is turned on
    else:
        pumpStatus=0
        PUMP=0
    if Height>CompMaxHeight*0.5:# Means if water level(Height) is less than 50% of computational max height(CompMaxHeight)
        if Height>=CompMaxHeight:#Means if water level (Height) greater than 100% of computational max height(CompMaxHeight)
            tank_height100=0
        elif Height<=CompMaxHeight*0.9:
            tank_height100=1
        if(pumpStatus==1 and tank_height100==0):
            PUMP=0

    else:
        if Height<=CompMaxHeight*0.25:#Means if water level (Height) greater than 100% of computational max height(CompMaxHeight)
            tank_height25=1
        elif Height>=CompMaxHeight*0.4:
            tank_height25=0
        if(pumpStatus==0 and tank_height25==1):
            PUMP=1
    if PUMP==1:
        return 1
    elif PUMP==0:
        return 0



    
#FLOW SENSOR CODE START  ##################################################################################
GPIO.setmode(GPIO.BOARD)
#Declaration of pin
flowSen1=29
flowSen2=31
flowSen3=7
flowSen4=11
tankIn=12
tankOut=13

#Declaration of calibration factor for each Flow sensor
cali_flowSen1=4.5
cali_flowSen2=4.5
cali_flowSen3=4.5
cali_flowSen4=4.5
cali_tankIn=4.5
cali_tankOut=4.5

global pulsecount_flowSen1
pulsecount_flowSen1=0
global pulsecount_flowSen2
pulsecount_flowSen2=0
global pulsecount_flowSen3
pulsecount_flowSen3=0
global pulsecount_flowSen4
pulsecount_flowSen4=0
global pulsecount_tankIn
pulsecount_tankIn=0
global pulsecount_tankOut
pulsecount_tankOut=0

#dEFINING SENSOR INPUT PIN
GPIO.setup(flowSen1,GPIO.IN,pull_up_down=GPIO.PUD_UP)
GPIO.setup(flowSen2,GPIO.IN,pull_up_down=GPIO.PUD_UP)
GPIO.setup(flowSen3,GPIO.IN,pull_up_down=GPIO.PUD_UP)
GPIO.setup(flowSen4,GPIO.IN,pull_up_down=GPIO.PUD_UP)
GPIO.setup(tankIn,GPIO.IN,pull_up_down=GPIO.PUD_UP)
GPIO.setup(tankOut,GPIO.IN,pull_up_down=GPIO.PUD_UP)

#Pulse Counter funtion for each Flowsensor
def pulse_flowSen1(flowSen1):
    global pulsecount_flowSen1
    pulsecount_flowSen1=pulsecount_flowSen1+1

def pulse_flowSen2(flowSen2):
    global pulsecount_flowSen2
    pulsecount_flowSen2=pulsecount_flowSen2+1

def pulse_flowSen3(flowSen3):
    global pulsecount_flowSen3
    pulsecount_flowSen3=pulsecount_flowSen3+1

def pulse_flowSen4(flowSen4):
    global pulsecount_flowSen4
    pulsecount_flowSen4=pulsecount_flowSen4+1
    
def pulse_tankIn(tankIn):
    global pulsecount_tankIn
    pulsecount_tankIn=pulsecount_tankIn+1

def pulse_tankOut(tankOut):
    global pulsecount_tankOut
    pulsecount_tankOut=pulsecount_tankOut+1

    
def flowSensor():
    global pulsecount_flowSen1
    pulsecount_flowSen1=0
    global pulsecount_flowSen2
    pulsecount_flowSen2=0
    global pulsecount_flowSen3
    pulsecount_flowSen3=0
    global pulsecount_flowSen4
    pulsecount_flowSen4=0
    global pulsecount_tankIn
    pulsecount_tankIn=0
    global pulsecount_tankOut
    pulsecount_tankOut=0



    
    time.sleep(1)
    

    
    flowRate1=pulsecount_flowSen1/cali_flowSen1
    flowRate2=pulsecount_flowSen2/cali_flowSen2
    flowRate3=pulsecount_flowSen3/cali_flowSen3
    flowRate4=pulsecount_flowSen4/cali_flowSen4
    flowRateTankIn=pulsecount_tankIn/cali_tankIn
    flowRateTankOut=pulsecount_tankOut/cali_tankOut
    if flowRateTankIn>flowRateTankOut:#Since flow sensor will give some value in opposit direction so we need counter that
        tankFlowRate=-flowRateTankIn
    else:
        tankFlowRate=flowRateTankOut
    print ("FR1=",round(flowRate1,3)," FR2=",round(flowRate2,3)," FR3=",round(flowRate3,3)," FR4=",round(flowRate4,3)," TankFlow=",round(tankFlowRate,3))

##    flowRate1=3
##    flowRate2=4
##    flowRate3=5
##    flowRate4=1
##    tankFlowRate=2
        
    waterConsumtionRate=round(flowRate1,3)+round(flowRate2,3)+round(flowRate3,3)+round(flowRate4,3)
    print (waterConsumtionRate)
    return [waterConsumtionRate,tankFlowRate]
#FLOW SENSOR CODE END  ####################################################################################



#RELAY SWITCH CODE START ##################################################################################
pumpPin=16
GPIO.setup(pumpPin,GPIO.OUT)
GPIO.output(pumpPin,True)#At the starting of the program pump will be off
def relaySwitch(x):
    if x==1:
        GPIO.output(pumpPin,False)#Pump is turned ON
        print '\n','PUMP ON'
        return 1
    else:
        GPIO.output(pumpPin,True)#Pump is turned OFF
        print '\n','PUMP OFF'
        return 0
#RELAY SWITCH CODE END #####################################################################################



def tim2sec(time):# Function to convert time to second
    return time.hour*3600+time.minute*60+time.second
def fileName(c,x):#this function is to returm name and location of text file. name will be associated with cycle no (c) and date(x)
    name='/home/pi/important_file/text_file/'+'C'+str(c)+'_'+str(x.day)+'_'+str(x.month)+'_'+str(x.year)+'.txt'
    return name
#WATER LEVEL IN THE TANK START ##############################################################################

echo=35  #echo is input pin of ultrasonic sensor
trig=37  # trig is output pin of ultra sonic sensor to send pulse
GPIO.setup(echo,GPIO.IN)
GPIO.setup(trig,GPIO.OUT)
def echoTime(x):#This function will calculate travelling time of sound from sensor to water surface Argument 'x' is number of reading to have average value
     numOfReading=x
     count=numOfReading
     Tim=0.0
     while count>0:
          GPIO.output(trig,False)
          time.sleep(0.002)
          GPIO.output(trig,True)
          time.sleep(2e-6)
          GPIO.output(trig,False)

          to=time.time()
          while GPIO.input(echo)==0:
               startTime=time.time()
               if time.time()-to>1:
                  break
               
          t1=time.time()
          while GPIO.input(echo)==1:
               endTime=time.time()
               if time.time()-t1>1:
                 break

          Tim=Tim+(endTime-startTime)/2
          count=count-1
     return (Tim/numOfReading)
#WATER LEVEL IN THE TANK END ##############################################################################

TAG1=1
TAG2=2
TAG3=3



if rank==1:
    while True:
        Rec_textFileName=comm.recv(source=0,tag=TAG3)#This will wait for order, that cycle has complete now calculate threshold flow rate and send this
        #if TAG3==4:#Because of while loop we need to flip the tag to secure that previous data should be received
            #TAG3=5
        #elif TAG3==5:
            #TAG3=4
        
        dataFile=open(Rec_textFileName,'r')
        Time=[]
        FlowData1=[]# instantaneous consumption rate
        FlowData2=[]# Flow rate value of sensor near to upper head tank
        pumpStatus=[]# It will tell that whether pump is on(1) or off(0) that instant
        s_t=datetime.now()
        for line in dataFile:
            #print line
            pos_semi1=line.find(":")# Position of first semicolon
            pos_semi2=line.find(":",pos_semi1+1)# Position of 2nd semicolon
            pos_semi3=line.find(":",pos_semi2+1)# Position of 3rd semicolon
            pos_semi4=line.find(":",pos_semi3+1)# Position of 4th semicolon
            pos_semi5=line.find(":",pos_semi4+1)# Position of 5th semicolon
            Hour=int(float(line[0:pos_semi1]))# Extracting the hours value
            minute=int(float(line[pos_semi1+1:pos_semi2]))# Extracting the minutes value
            Second=int(float(line[pos_semi2+1:pos_semi3]))# Extracting the Second value
            FlowRate1=float(line[pos_semi3+1:pos_semi4])# Extracting data of the instantaneous consumption rate of building.
            FlowRate2=float(line[pos_semi4+1:pos_semi5])# Extracting data of the instantaneous flow rate of sensor near to upperhead tank which may be positive or negative.To the tank -ve and from the tank +ve
            motorStatus=int(line[pos_semi5+1:])# Getting the status of motor whether motor is on(1) or off(0) at this time instant. This information is required to calculate the total consumed from the tank during pumping
            Time=Time+[Hour*3600+minute*60+Second]# Array of Time
            FlowData1=FlowData1+[FlowRate1]# Array of total consumed FlowRate of building Data
            FlowData2=FlowData2+[FlowRate2]# Array of flowrate data of sensor near to upper head tank
            pumpStatus=pumpStatus+[motorStatus]# Array of motor status
        dataFile.close()
        threshold=NDFRST(FlowData1,Time,dutyCycle,tolerance,increasing_fac,decreasing_fac)
        waterConsumption=waterConsum(FlowData1,FlowData2,pumpStatus)
        threshold=[threshold[0]]+[waterConsumption[0]]+[waterConsumption[1]]
        data1=numpy.array(threshold)
        req=comm.Isend([data1,3,MPI.DOUBLE],dest=0,tag=TAG1)
        aadi=comm.send(data1,dest=0,tag=TAG2)
        if TAG1==0:#Because of while loop we need to flip the tag to secure that previous data should be sent
            TAG1=1
        elif TAG1==1:
            TAG1=0
        if TAG2==2:
            TAG2=3
        elif TAG2==3:
            TAG2=2
    
if rank==0:
    os.system('clear')
    while True:
        print '\n', 'Would you like to calibrate the Ultrasonic Sensor','\n'
        print 'If Yes Please Enter [y/Y] else [n/N]'
        takeInput=raw_input()
        if takeInput=='y' or takeInput=='Y':
            #CALIBRATION OF ULTRASONIC SENSOR START #######################
            while True:
                 print "\n","Please Empty the tank completely and calibrate the Bottom Water Level","\n","\n"
                 print 'If you have done above step, Enter [y/Y]'
                 userIn=raw_input()
                 if userIn=='y' or userIn=='Y':
                      T1=echoTime(1)
                      calibrationFile=open('/home/pi/important_file/text_file/CalibrationFile/Calibration.txt','w')
                      calibrationFile.write(str(T1))
                      calibrationFile.write("\n")
                      calibrationFile.close()
                      print '\n','Bottom Water Level Calibration DONE'
                      break

            #CALIBRATION OF UPPER WATER LEVEL START ##############################
                      
            
            while True:
                 print "\n","Please Fill the tank and calibrate the upper Water Level","\n","\n"
                 
                 while True:
                      print 'To Switch ON the PUMP, Enter 1 and to switch off the PUMP enter 0'
                      pumpInput=input();
                      if pumpInput==1:
                        relaySwitch(pumpInput)
                      elif pumpInput==0:
                        relaySwitch(pumpInput)
                        break
                      
                 print 'If you have done above step, Enter [y/Y]'
                 userIn=raw_input()
                 if userIn=='y' or userIn=='Y':
                      T2=echoTime(1)
                      calibrationFile=open('/home/pi/important_file/text_file/CalibrationFile/Calibration.txt','a')
                      calibrationFile.write(str(T2))
                      calibrationFile.close()
                      print '\n','Upper Water Level Calibration DONE'
                      break
            break
        elif takeInput=='n' or takeInput=='N':
            calibrationFile=open('/home/pi/important_file/text_file/CalibrationFile/Calibration.txt','r')
            temp2=0
            for line in calibrationFile:
                if temp2==0:
                    T1=float(line)
                    print 'T1=',T1,'\n'
                    temp2=1
                else:
                    T2=float(line)
                    print 'T2=',T2,'\n'
            break

    #CALIBRATION OF ULTRASONIC SENSOR START #######################
    waterLevelPre=int(120*(echoTime(1)-T1)/(T2-T1))#Water level from previous step
    startTime=datetime.now()#Now program has started
    Cycle_num=0
    
    temp=0
    ThreFlowRate=13 #Threshold flow rate in liter per min
    newFileName=fileName(Cycle_num,startTime)
    NewFile=open(newFileName,'w')#Creating new file to write sensor data
    req=comm.Irecv(data1,source = 1,tag=TAG1)
    re=False

    
    GPIO.add_event_detect(flowSen1,GPIO.FALLING,callback=pulse_flowSen1)
    GPIO.add_event_detect(flowSen2,GPIO.FALLING,callback=pulse_flowSen2)
    GPIO.add_event_detect(flowSen3,GPIO.FALLING,callback=pulse_flowSen3)
    GPIO.add_event_detect(flowSen4,GPIO.FALLING,callback=pulse_flowSen4)
    GPIO.add_event_detect(tankIn,GPIO.FALLING,callback=pulse_tankIn)
    GPIO.add_event_detect(tankOut,GPIO.FALLING,callback=pulse_tankOut)
    
    while True:
        TIME=datetime.now()
        if (tim2sec(TIME)-tim2sec(startTime))>=CycleTime:#Means when one cycle has complete then call to calculate threshold flow rate in core 1
            NewFile.close()#Closing previous file
            oldFileName=fileName(Cycle_num,startTime)#This name will be send to core 1 to read this file to calculate threshold flow rate
            callForThre=comm.send(oldFileName,dest=1,tag=TAG3)#Sending file name to read and calculate threshold flow rate
            Cycle_num=Cycle_num+1#Go to next cycle
            startTime=TIME#Now new cycle has been started
            newFileName=fileName(Cycle_num,startTime)
            NewFile=open(newFileName,'w')#Creating new file to write sensor data
            #if TAG3==4:#Because of while loop we need to flip the tag to secure that previous data should be sent
                #TAG3=5
            #elif TAG3==5:
                #TAG3=4

        
        re=MPI.Request.Test(req)
        if re==False :
            waterLevelNew=int(100*(echoTime(1)-T1)/(T2-T1))#New Water level
            ## because of fluctuation i need to do following steps
            if waterLevelNew>120 or waterLevelNew<0:
                waterLevelNew=waterLevelPre
            else :
                waterLevelPre=waterLevelNew
                
            
            flowSensorData=flowSensor()
            
            print '\n','Water Lavel',waterLevelNew,'%'
            currentTime=datetime.now()
            PumpON_OFF_status=relaySwitch(Pump_OnOff(ThreFlowRate,flowSensorData[0],waterLevelNew))#Calling function to turn on/off the pump based on condition

            #Writing above value into the NewFile
            NewFile=open(newFileName,'a')
            NewFile.write(str(currentTime.hour))
            NewFile.write(":")
            NewFile.write(str(currentTime.minute))
            NewFile.write(":")
            NewFile.write(str(currentTime.second))
            NewFile.write(":")
            NewFile.write(str(round(flowSensorData[0],2)))
            NewFile.write(":")
            NewFile.write(str(round(flowSensorData[1],2)))
            NewFile.write(":")
            NewFile.write(str(PumpON_OFF_status))
            NewFile.write("\n")
            NewFile.close()

        else :
            data=comm.recv(source=1,tag=TAG2)
            ThreFlowRate=data[0]#Threshold flow rate for next day
            ThreFile=open('/home/pi/important_file/text_file/CalibrationFile/thre.txt','a')
            ThreFile.write(str(ThreFlowRate))
            ThreFile.write(str('\n'))
            ThreFile.close()
            ToWatCon=data[1]#Total Water Consumed
            print "ThreFlowRate=",ThreFlowRate
            puTimWaCon=data[2]#Total water drawn from the tank during pumping time

            if TAG1==0:#Because of while loop we need to flip the tag to secure that previous data should be received
                TAG1=1
            elif TAG1==1:
                TAG1=0
            if TAG2==2:
                TAG2=3
            elif TAG2==3:
                TAG2=2
            req=comm.Irecv(data1,source=1,tag=TAG1)
