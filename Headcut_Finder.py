import arcpy
import os
from arcpy import env
from arcpy.sa import *
from arcpy import da

arcpy.env.overwriteOutput = True
arcpy.SetLogHistory(False)
arcpy.CheckOutExtension("spatial")

polyline = arcpy.GetParameterAsText(0)
inputDEM=arcpy.GetParameterAsText(1)
userdistance=arcpy.GetParameterAsText(2)
env.workspace=arcpy.GetParameterAsText(3)
naming=arcpy.GetParameterAsText(4)

spatial_ref = arcpy.Describe(polyline).spatialReference
output_points=os.path.join(naming+"_"+"output_points")
##headcuts=os.path.join(env.workspace,naming+"_"+"suspected_headcuts")

mem_lines=arcpy.CopyFeatures_management(polyline,r"in_memory/lines")
mem_point = arcpy.CreateFeatureclass_management(r"in_memory", "points_in_memory", "POINT", "", "DISABLED", "DISABLED", polyline)

##arcpy.CalculateField_management(polyline,"Direction_of_Flow",

#add line object id and value to the point feature class
arcpy.AddField_management(mem_point, "LineOID", "LONG")
arcpy.AddField_management(mem_point, "Value", "FLOAT")

#get count of polylines
result = arcpy.GetCount_management(mem_lines)

#making result into an integer?
features = int(result.getOutput(0))

search_fields = ["SHAPE@", "OID@"]
insert_fields = ["SHAPE@", "LineOID", "Value"]

#####put points on lines########################################################################

# makes search cursor and insert cursor into search/insert
with arcpy.da.SearchCursor(mem_lines, (search_fields)) as search:
    with arcpy.da.InsertCursor(mem_point, (insert_fields)) as insert:
        for row in search:
            
#line geom is shape in search fields
                line_geom = row[0]
                length = float(line_geom.length)
                distance = 0
                
#oid is OID@ in search fields            
                oid = str(row[1])

                #creates a point at the start and end of line for final point that is less then the userdistance away
                start_of_line = arcpy.PointGeometry(line_geom.firstPoint)
                end_of_line = arcpy.PointGeometry(line_geom.lastPoint)

                #returns a point on the line at a specific distance from the beginning
                point = line_geom.positionAlongLine(distance, False)

                #insert point at every userdistance
                while distance <= length:
                    point = line_geom.positionAlongLine(distance, False)
                    insert.insertRow((point, oid, float(distance)))
                    distance += float(userdistance)
                    
                insert.insertRow((end_of_line, oid, length))

del search
del insert

arcpy.Delete_management(mem_lines)

##inputDEM=arcpy.CopyRaster_management(DEM,r"in_memory/a")

# transfer elevation to points

ExtractMultiValuesToPoints(mem_point, [[inputDEM, "Elevation"]])

##arcpy.Delete_management(inputDEM)

# calculate elevation change

arcpy.AddField_management(mem_point,"Ev_Change","FLOAT")

feilds=["Elevation","LINEOID","Ev_Change","OID@"]

elev1=0
line_OID=1

with arcpy.da.UpdateCursor(mem_point,feilds) as update:
        
        for row in update:

                if(row[3]==1):

                        row[2]=float(0)
                        elev1=float(row[0])

                elif(line_OID==row[1]):
                        elev2 = float(row[0])
                        row[2]=(elev1-elev2)
                        
                        elev1=elev2
                        
                        OID=row[1]
                        
                        update.updateRow(row)

                else:
                        row[2]=0
                        line_OID=row[1]
                        
arcpy.CopyFeatures_management(mem_point,output_points)

#make mem_points a layer so select by attibutes can be done

layer=arcpy.MakeFeatureLayer_management(mem_point,"output_points.lyr")

arcpy.SelectLayerByAttribute_management(layer, "NEW_SELECTION", "Ev_Change >0.0508 OR Ev_Change <-0.0508")

arcpy.FeatureClassToFeatureClass_conversion(layer,env.workspace,naming+"_"+"suspected_headcuts")

arcpy.Delete_management(mem_point)

arcpy.Delete_management(layer)

