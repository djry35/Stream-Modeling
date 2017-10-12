#Written by Daniel Raymon and Ryan Wortmann, MDC GIS programmers. 
#
#Date finished: 
#
#Script takes an arcGIS feature layer of points that make up a watershed or group of watersheds, 
#and extracts the points that make up only the active channel (AC) of the streams present. 
#The resulting list will be a list of ObjectIDs referencing points on the watershed feature class,
#as well as a polygon set created from those points. 
#


#---------------------------------------IMPORTANT NOTE-------------------------------------------------------
#Script uses Multiprocessing paradigms. Changing the wrong things in this program can crash a computer very easily.

#Also, this assumes the computer processing this stuff has the ability to multiprocess.
#---------------------------------------IMPORTANT NOTE-------------------------------------------------------




import os
import sys
import time
from itertools import repeat

if __name__ == '__main__':
    print("Importing libraries...")

try:
    import arcinfo
    import arcpy
    arcpy.env.overwriteOutput = True
    arcpy.SetLogHistory(False)
except ImportError as IE:
    print("Your computer doesn't have the required libraries to run arcpy tools. Library required: arcpy\n")
        
    raw_input("\nHit Enter to Continue...")
    sys.exit()

else:
    from arcpy import env

try:
    import multiprocessing
    from multiprocessing import Process, Lock, Array, Value
except ImportError as IE:
    try:
        import multiprocessing
    except ImportError as IE:
        print("Your computer doesn't have the required libraries to run IPC. Library required: multiprocessing")
        raw_input("\nHit Enter to Continue...")
        sys.exit()
    else:
        print("Weird. I found the multiprocessing library, but couldn't import secondary modules I need. Contact developers!")
        raw_input("\nHit Enter to Continue...")
        sys.exit()

if __name__ == '__main__':
    print("Done.")

#The minimum slope that needs to be found on the bank edge before calculating AC points.
#The lower the value the more accurate the actual AC will be, but more extraneous points will be selected as AC.
MIN_SLOPE_REQUIRED = 30

#Modified down below
SECTION_SIZE = -1


#Value is in cells. So this number * cell resolution = max width in meters
MAX_STREAM_WIDTH = 20



#These are all enumerated values for the purposes of understanding what flags are doing
#throughout the program. Flags are used to communicate between processes.
#The actual numbers do not mean anything; the variables are just used to translate certain actions to english.
CHILD_GO = 0
PARENT_GO = 1
PARENT_BUSY = 3
NO_MORE_DATA = 4
CHILD_ERROR = 3333333 

PROCS_GOING = 44
NO_PROCS_GOING = 45

tempIndex = 0


#The bigger this is, the more gaps will be filled in if any, but the less accurate it will be overall. 
POLYGON_AGGREGATE_DISTANCE = "8 meters"

#Location of all the data. Should be modified accordingly through manual entry into this script, or through a wrapper/frontend.
DATABASE=r"N:\Wortmr\Bankfull_tool\Bankfull_tool_copy_copy.gdb"
POINT_DATASET_NAME=r"N:\Wortmr\Bankfull_tool\Bankfull_tool_copy_copy.gdb\watershed_8_points"
OUTPUT_DATASET_NAME=os.path.join(DATABASE,"W8_8_23")
OUTPUT_POLYGONS_NAME=OUTPUT_DATASET_NAME + "_polys"
ELEVATION_COLUMN_NAME="Elevation"
SLOPE_COLUMN_NAME="Slope"
CURRENT_DATE=time.strftime("%B_%d")
RESOLUTION = .5


class Point:
    def __init__(self, ID, xVal, yVal):
        self.ID = ID
        self.xVal = xVal
        self.yVal = yVal

def getACdataset(Flag, MinID, MaxID, layerLock, ID):

    #Sets up the workspace the child will use.
    mem_points_child_workspace=r"in_memory\tempACdata" + str(ID)
    high_slope_memory = r"in_memory\tempACdataHighSlope" + str(ID)

    randomVal = 0
    randomVal2 = 0

    #While there's data to process...
    while True:
        #Child waits for parent to say go.
        while Flag.value != CHILD_GO and Flag.value != NO_MORE_DATA:
            pass

        if Flag.value == NO_MORE_DATA:
            break

        minID = MinID.value
        maxID = MaxID.value

        if randomVal % 26 == 0:
            randomVal2 += 1
            
        Points = []
                    
        indexID = minID

        try:
            layerLock.acquire()
            arcpy.MakeFeatureLayer_management(POINT_DATASET_NAME, mem_points_child_workspace, "OBJECTID <= {} AND OBJECTID >= {}".format(maxID,minID))
            layerLock.release()
        except Exception:
            Flag.value = CHILD_ERROR
            return
        

        search_fields=[SLOPE_COLUMN_NAME, ELEVATION_COLUMN_NAME, "POINT_X", "POINT_Y", "pointid"]

        arcpy.SelectLayerByAttribute_management(mem_points_child_workspace, "NEW_SELECTION", "{} >= {}".format(SLOPE_COLUMN_NAME, "30"))

        if int(arcpy.GetCount_management(mem_points_child_workspace).getOutput(0)) == 0:
            arcpy.Delete_management(mem_points_child_workspace)
            Flag.value = PARENT_GO
            continue

        arcpy.CopyFeatures_management(mem_points_child_workspace, high_slope_memory)

        arcpy.SelectLayerByAttribute_management(mem_points_child_workspace, "CLEAR_SELECTION")

        with arcpy.da.SearchCursor(mem_points_child_workspace, search_fields) as searchAll:
            with arcpy.da.SearchCursor(high_slope_memory, search_fields) as searchHighSlope:
                try:
                    pointA = searchHighSlope.next()
                    pointB = searchHighSlope.next()
                    pointC = searchAll.next()
                    global tempIndex
                    while True:
                        if isclose(float(pointB[3]), float(pointA[3])):
                            if float(pointB[2]) - float(pointA[2]) < 2*RESOLUTION:
                                pass
                            elif float(pointB[2]) - float(pointA[2]) < MAX_STREAM_WIDTH:
                                tempIndex = indexID
                                pointC = SyncPoints(pointC, searchAll, pointA)
                                indexID = tempIndex
                                
                                pointC = searchAll.next()
                                indexID += 1
                                
                                while not isclose(float(pointC[2]),float(pointB[2])):
                                    if float(pointC[0]) <= 10:
                                        if float(pointC[1]) <= float(pointA[1]) and float(pointC[1]) <= float(pointB[1]):
                                            Points.append(Point(indexID, float(pointC[2]), float(pointC[3])))
                                        elif float(pointC[1]) <= float(pointA[1]) or float(pointC[1]) <= float(pointB[1]):
                                            if existsAdjacent(pointC, Points):
                                                Points.append(Point(indexID, float(pointC[2]), float(pointC[3])))
                                            
                                    pointC = searchAll.next()
                                    indexID += 1
                            else:
                                tempIndex = indexID
                                pointC = SyncPointsClose(pointC, searchAll, pointA)
                                indexID = tempIndex

                                while abs(float(pointC[2]) - float(pointA[2])) < MAX_STREAM_WIDTH:
                                    if float(pointC[1]) <= float(pointA[1]) and float(pointC[0]) <= 10:
                                        if existsAdjacent(pointC, Points):
                                            Points.append(Point(indexID, float(pointC[2]), float(pointC[3])))
                                    pointC = searchAll.next()
                                    indexID += 1

##                                tempIndex = indexID
##                                pointC = SyncPointsClose(pointC, searchAll, pointB)
##                                indexID = tempIndex
##
##                                while abs(float(pointC[2]) - float(pointB[2])) < MAX_STREAM_WIDTH:
##                                    if float(pointC[1]) <= float(pointB[1]) and float(pointC[0]) <= 10:
##                                        if existsAdjacent(pointC, Points):
##                                            Points.append(Point(indexID, float(pointC[2]), float(pointC[3])))
##                                    pointC = searchAll.next()
##                                    indexID += 1

                            pointA = pointB
                            pointB = searchHighSlope.next()
                            continue

                        else:
                            tempIndex = indexID
                            pointC = SyncPointsClose(pointC, searchAll, pointA)
                            indexID = tempIndex   

                            while abs(float(pointC[2]) - float(pointA[2])) < MAX_STREAM_WIDTH:
                                if float(pointC[1]) <= float(pointA[1]) and float(pointC[0]) <= 10:
                                    if existsAdjacent(pointC, Points):
                                        Points.append(Point(indexID, float(pointC[2]), float(pointC[3])))
                                    
                                pointC = searchAll.next()
                                indexID += 1


                            while abs(float(pointB[3]) - float(pointC[3])) > 2*RESOLUTION:
                                tempIndex = indexID
                                pointC = SyncPointsCloseOnX(pointC, searchAll, pointA)
                                indexID = tempIndex

                                while abs(float(pointC[2]) - float(pointA[2])) < MAX_STREAM_WIDTH:
                                    if float(pointC[1]) <= float(pointA[1]) and float(pointC[0]) <= 10:
                                        if existsAdjacent(pointC, Points):
                                            Points.append(Point(indexID, float(pointC[2]), float(pointC[3])))
                                    
                                    pointC = searchAll.next()
                                    indexID += 1

                                tempIndex = indexID
                                pointC = getToNewYvalue(pointC, searchAll)
                                indexID = tempIndex
                            
##                            tempIndex = indexID
##                            pointC = SyncPointsClose(pointC, searchAll, pointB)
##                            indexID = tempIndex   
##
##                            while abs(float(pointC[2]) - float(pointB[2])) < MAX_STREAM_WIDTH:
##                                if float(pointC[1]) <= float(pointB[1]) and float(pointC[0]) <= 10:
##                                    Points.append(Point(indexID, float(pointC[2]), float(pointC[3])))
##                                    
##                                pointC = searchAll.next()
##                                indexID += 1
                                
                            pointA = pointB
                            pointB = searchHighSlope.next()
                            continue
                            
                            
                except StopIteration:
                        pass

        if len(Points) == 0:
            arcpy.Delete_management(mem_points_child_workspace)
            arcpy.Delete_management(high_slope_memory)
            Flag.value = PARENT_GO
            randomVal += 1
            continue
        
        ACdataset = list(( item.ID for item in Points ))

        

        #Massaging data into a format that arcpy functions will like.
        string = str(ACdataset)
        QS = convertStringToQueryString(string)

        #MAKE LAYER FROM AC LIST
        arcpy.SelectLayerByAttribute_management(mem_points_child_workspace, "CLEAR_SELECTION")
        arcpy.SelectLayerByAttribute_management(mem_points_child_workspace, "NEW_SELECTION", "OBJECTID IN " + QS)

        try:
            layerLock.acquire()
            arcpy.CopyFeatures_management(mem_points_child_workspace, os.path.join(DATABASE, CURRENT_DATE + "_" + numsToAlpha(ID, randomVal, randomVal2) + "_PTs"))
            layerLock.release()
        except Exception as e:
            print e
            Flag.value = CHILD_ERROR
            return
       
        randomVal += 1
        
        
        Flag.value = PARENT_GO
        
        arcpy.Delete_management(mem_points_child_workspace)
        arcpy.Delete_management(high_slope_memory)


def existsAdjacent(pointReference, pointList):
    leftRight = RESOLUTION * -1
    while leftRight <= RESOLUTION:
        upDown = RESOLUTION * -1
        while upDown <= RESOLUTION:
            if any(p for p in pointList if isclose(float(p.yVal), float(pointReference[3]) + float(upDown)) and isclose(float(p.xVal), float(pointReference[2]) + float(leftRight))):
                return True
            upDown += RESOLUTION

        leftRight += RESOLUTION

    return False

def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):
    return abs(a-b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)        

def getToNewYvalue(currentPoint, cursor):
    global tempIndex
    prevY = float(currentPoint[3])
    while prevY == float(currentPoint[3]):
        currentPoint = cursor.next()
        tempIndex += 1

    return currentPoint

def SyncPoints(currentPoint, cursor, pointToFind):
    global tempIndex
    while float(currentPoint[2]) != float(pointToFind[2]) or float(currentPoint[3]) != float(pointToFind[3]):
        currentPoint = cursor.next()
        tempIndex += 1
    return currentPoint

def SyncPointsClose(currentPoint, cursor, pointToFind):
    global tempIndex
    while float(currentPoint[3]) != float(pointToFind[3]):
        currentPoint = cursor.next()
        tempIndex += 1

    while abs(float(currentPoint[2]) - float(pointToFind[2])) >= MAX_STREAM_WIDTH:
        currentPoint = cursor.next()
        tempIndex += 1
        
    return currentPoint

def SyncPointsCloseOnX(currentPoint, cursor, pointToFind):
    global tempIndex

    while abs(float(currentPoint[2]) - float(pointToFind[2])) >= MAX_STREAM_WIDTH:
        currentPoint = cursor.next()
        tempIndex += 1
        
    return currentPoint

def numsToAlpha(num1, num2, num3):
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    alphaList = list(alphabet)
    return alphaList[num1 % 26] + alphaList[num2 % 26] + alphaList[num3 % 26] 


def convertStringToQueryString(string):
    almost=string.replace("[","")
    there=almost.replace("]","")
    fixed="("+there+")"
    return fixed


def printProgressBar(percent):
    sys.stdout.write('\r')
    sys.stdout.write("[%-20s] %.2f%%" % ('='*int(percent/5), percent))
    sys.stdout.flush()

def determineNumProcs(totalPoints):
    global SECTION_SIZE
    numCores = int(multiprocessing.cpu_count())
    numProcs = -1
    #I think though the cursors work too fast to be able to run more than 6 at a time. Oh well.
    if numCores >= 16:
        numSections = 200
        numProcs = 6
    elif numCores >= 6:
        numSections = 100
        numProcs = 4
    elif numCores >= 4:
        numSections = 50
        numProcs = 2
    else:
        print("WARNING: this CPU cannot multitask very well. Running this process on this machine might be quite slow.")
        numSections = 10
        numProcs = 1
        
    while int(totalPoints) / int(numSections) == 0:
        numSections -= 1
    SECTION_SIZE = int(int(totalPoints) / int(numSections))
    
    return numProcs

#If you didn't know what you were doing up at the children, you would ideally start here.

#This is where the actual program starts. This code will spawn children that will all run the code above,
#and this code will just be used to manage all those children.
if __name__ == '__main__':
    
    arcpy.AddMessage("\n\nREMEMBER: DO NOT ATTEMPT TO USE THIS PROCESS IN ARCPY AS A TOOL!\nIT WILL NOT WORK!\n\n")
    
    time.sleep(2)

    
    print("Locating feature layer...")

    try:
        mem_point=arcpy.MakeFeatureLayer_management(POINT_DATASET_NAME,"pointlayer")
    except Exception:
        print("Couldn't find " + POINT_DATASET_NAME + ".\n\nDoes it exist?")
        e = sys.exc_info()[1]
        print("\n\nArcpy says: \n" + e.args[0])
        raw_input("\nHit Enter to Continue...")
        sys.exit()


    total_points=arcpy.GetCount_management(mem_point).getOutput(0)

    arcpy.Delete_management(mem_point)
    
    print("Done.\n")

    print("Collecting IPC tools...")

    try:
        NUM_PROCS = determineNumProcs(total_points)
            
        Flags = [Value('i', PARENT_BUSY, lock=False) for i in repeat(None, NUM_PROCS)]

        minIDs = [Value('i', 0) for i in repeat(None, NUM_PROCS)]
        maxIDs = [Value('i', 0) for i in repeat(None, NUM_PROCS)]

        #Used to lock the database for asynchronous copying/reading. Short testing has found that asynchronous is faster than synchronous overall, which is kinda weird
        #since the only operations the children are doing are reads (writes require locking). 
        layerLock = Lock()
    except Exception as e:
        print("\nCouldn't create IPC tools. This is an unknown error!")
        print("\n\nError says\n" + str(e))
        raw_input("\nHit Enter to Continue...")
        sys.exit()

    processList = []
    
    for i in xrange(NUM_PROCS):
        p = Process(target=getACdataset, args=(Flags[i], minIDs[i], maxIDs[i], layerLock, i, ))
        p.start()
        processList.append(p)

    print("Done.\n")
                                                                      
    minIDs[0].value = 0
    maxIDs[0].value = minIDs[0].value + SECTION_SIZE
    
    print("Begin analysis (Will take a second to start going)...")

    #More micromanaging.
    test = PROCS_GOING
    doneFlag = 0
    newMax = -1
    placeholder = 0
    percentage = 0

    error = 0
    
    printProgressBar(0)


    #Here, the children get their initial assignment so they know where to start. 
    for i in xrange(NUM_PROCS):
        if i == 0:
            Flags[i].value = CHILD_GO
            continue
        minIDs[i].value = maxIDs[i-1].value + 1
        maxIDs[i].value = minIDs[i].value + SECTION_SIZE
        Flags[i].value = CHILD_GO

    while True:
        test = NO_PROCS_GOING
        #So now that the children are going, we'll just check in on every child to see if they're done. 
        for i in xrange(NUM_PROCS):

            
            if Flags[i].value == NO_MORE_DATA:
                continue

            elif Flags[i].value == CHILD_ERROR:
                print("\n\nOH NO! A child process bombed out. There might be a schema lock on the database.\nIs someone else using it?")
                print("\nI will now stop everything and collect what data I can.")
                print("\nYou will have to contact developers on this one. The scripting is too complex to solve otherwise :(\n")
                for p in processList:
                    p.terminate()
                    sleep(1)
            else:
                test = PROCS_GOING
                #The child will tell the parent when data is ready.
                if Flags[i].value == PARENT_GO:
                #So here, the child is paused until the parent tells it to go again.


                    percentage = float(minIDs[i].value * 100) / float(total_points)
                    
                    if placeholder != percentage:
                        if placeholder < percentage:
                            printProgressBar(percentage)
                            placeholder = percentage
                        
                    #Figure out what the child's new assignment is.
                    #Since the child that just finished might not have the most recent section dished out,
                    #we have to keep track of what we last assigned. 
                    if newMax == -1:
                        minIDs[i].value = maxIDs[NUM_PROCS - 1].value + 1
                    else:
                        minIDs[i].value = newMax + 1

                    maxIDs[i].value = minIDs[i].value + SECTION_SIZE
                    newMax = maxIDs[i].value

                    #Figure out if there's more data to be processed. It's okay if the IDs go over the max
                    #ID of the dataset, since we're just querying the dataset, so there's no effect.

                    #Can't set all flags to NO_MORE_DATA immediately since parent still has to wait for everyone to be done.
                    if int(maxIDs[i].value) > int(total_points) + int(SECTION_SIZE):
                        Flags[i].value = NO_MORE_DATA

                        doneFlag = 1
                    else:
                        #Just in case we don't get inside the above 'if' and we find that we're done.
                        if doneFlag == 1:
                            Flags[i].value = NO_MORE_DATA
                        else:
                            Flags[i].value = CHILD_GO
                    
        #The parent still has to wait until all the children are done.        
        if test == NO_PROCS_GOING:
            break

    printProgressBar(100)
    
    print("\n\nMerging sections...") 
    printProgressBar(0)
    
    try:
        env.workspace = DATABASE
        Files = []
        for F in arcpy.ListFeatureClasses(wild_card=CURRENT_DATE + "*_PTs", feature_type="Point"):
            Files.append(F)

        if len(Files) != 0:
            arcpy.Merge_management(Files, OUTPUT_DATASET_NAME)
            printProgressBar(25)
            
            for F in Files:
                arcpy.Delete_management(F)
            printProgressBar(50)
 
        else:
            print("\nSomething happened....No results were found. Is there something weird about the area being analyzed?")
            raw_input("\nHit Enter to Continue...")
            sys.exit()
            
    except Exception as IE:
        e = sys.exc_info()[1]
        print("\n\nError while merging results. This error can't be handled here.\n\nArcpy says:\n" + e.args[0])
        raw_input("\nHit Enter to Continue...")
        sys.exit()

    try:
        OUTPUT_POLYGONS_NAME=OUTPUT_DATASET_NAME + "_polys"
        arcpy.AggregatePoints_cartography(OUTPUT_DATASET_NAME, OUTPUT_POLYGONS_NAME, POLYGON_AGGREGATE_DISTANCE)
        printProgressBar(75)
    except Exception as IE:
        e = sys.exc_info()[1]
        print("\n\nError while creating polygons. This error can't be handled here.\n\nArcpy says:\n" + e.args[0])
        raw_input("\nHit Enter to Continue...")
        sys.exit()

    printProgressBar(100)
        
    print("\n\nDone.\n")

    #HUZZAH!
    print("\nDataset successfully saved.\n")

    raw_input("\nHit Enter to Continue...")

#END MAIN PROCESS LOGIC

