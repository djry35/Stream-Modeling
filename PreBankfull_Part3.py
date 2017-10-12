import arcpy
import sys
import os
import string

DATABASE=arcpy.GetParameterAsText(0)

POINT_DATASET_NAME=arcpy.GetParameterAsText(1)
RESOLUTION=arcpy.GetParameterAsText(2)

OUTPUT_DATASET_NAME=arcpy.GetParameterAsText(3)
SCRIPT_PATH = arcpy.GetParameterAsText(4)

pythonPath = sys.executable

if DATABASE == "" or POINT_DATASET_NAME == "" or \
    OUTPUT_DATASET_NAME == "" or SCRIPT_PATH == "" or \
    RESOLUTION == "":
    arcpy.AddMessage("ERROR: One of the parameters was left out! I need all of them to do the setup.")
    sys.exit()



if "python.exe" not in pythonPath and "pythonw.exe" not in pythonPath:
    arcpy.AddMessage("\n\nYou did not uncheck 'Run Python script in process'!\nIf you definitely did, contact developers...something is wrong\n\n")
    sys.exit()

index = string.find(RESOLUTION, " ")
RESOLUTION = RESOLUTION[:index]

index = string.rfind(SCRIPT_PATH, "\\")
index += 1
                     
TMP_PATH = SCRIPT_PATH[:index] + "Bankfull-Copy.py"

if not arcpy.Exists(DATABASE):
    print("\n\nERROR: test failed when trying to locate the workspace:\n" + DATABASE + "\n\nDoes it exist? \n")
    sys.exit()

try:
    with arcpy.da.SearchCursor(POINT_DATASET_NAME, "*") as searchAll:
        pass
except Exception:
    e = sys.exc_info()[1]
    print("\n\nERROR: test failed when trying to verify the point dataset.\n\nArcpy says: \n" + e.args[0])
    sys.exit()

try:
    with open(SCRIPT_PATH, "r") as fp:
        with open(TMP_PATH, "w") as fp2:
            for line in fp:
                if "DATABASE=" in line:
                    fp2.write("DATABASE=r\"" + DATABASE + "\"\n")
                elif "POINT_DATASET_NAME=" in line:
                    fp2.write("POINT_DATASET_NAME=r\"" + POINT_DATASET_NAME + "\"\n")
                elif "OUTPUT_DATASET_NAME=" in line:
                    fp2.write("OUTPUT_DATASET_NAME=os.path.join(DATABASE, \"" + OUTPUT_DATASET_NAME + "\")\n")
                elif "POLYGON_AGGREGATE_DISTANCE=" in line:
                    fp2.write("POLYGON_AGGREGATE_DISTANCE=\"" + POLYGON_AGGREGATE_DISTANCE + "\"\n")
                elif "RESOLUTION=" in line:
                    fp2.write("RESOLUTION=" + RESOLUTION + "\n")  

                else:
                    fp2.write(line)
except Exception:
    arcpy.AddMessage("\nERROR: Could not find the script file: " + SCRIPT_PATH)
    sys.exit()

arcpy.AddMessage("\n\nThere should now be a file called Bankfull-Copy.py in the same location as the script I modified.\n")
arcpy.AddMessage("Location of script, as a reminder: " + SCRIPT_PATH[:index])
arcpy.AddMessage("\nGo ahead and put that file in the following location: " + pythonPath)
arcpy.AddMessage("\nOnce you do that, you should just be able to double click it and let it run!\n\n")

