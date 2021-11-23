'''
Copyright 2021 Esri

Licensed under the Apache License Version 2.0 (the "License");
you may not use this file except in compliance with the License.

You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

See the License for the specific language governing permissions and
limitations under the License.
''' 

# -*- coding: utf-8 -*-

import os.path
import os
import arcpy
import xml.etree.ElementTree as ET

class FixIntersectionsAsReferentsInLRSEvents(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Fix Intersections as Referents in LRS Events"
        self.description = ""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        params = []
        param0 = arcpy.Parameter(
            displayName="Old Intersection Feature Class",
            name="old_intersection_fc",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Input")
        params.append(param0)

        param1 = arcpy.Parameter(
            displayName="New Intersection Feature Class",
            name="new_intersection_fc",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Input")
        params.append(param1)

        param2 = arcpy.Parameter(
            displayName="LRS Event Feature Class",
            name="lrs_event_fc",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Input",
            multiValue=True)
        params.append(param2)

        return params

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        ValidateParameters(parameters)

        return

    def execute(self, parameters, messages):

        # Clear from any previous runs
        eventFCDict.clear()

        # Populate the events dictionary based on multivalue events parameter
        eventFC = parameters[2].valueAsText
        eventFCList = eventFC.split(";")
        PopulateEventsDict(eventFCList)

        # Set the coded values for old and new intersections
        continueProcessing = SetCodedValueDomains(parameters)

        if not continueProcessing:
            arcpy.AddMessage("Tool failed.  No events were processed.")
            return

        txtFile = OpenLogFile()

        for eventFC in eventFCDict:
            # Clear the container of events
            events_with_intersections.clear()

            # Query events table and update events_with_intersections
            UpdateEventsWithIntersections(eventFC)

            # Perform a spatial join on new and old intersections and update events table accordingly
            arcpy.SetProgressorLabel("Updating events in {}".format(eventFC))
            PerformSpatialJoinAnalysis(parameters, eventFC)

            arcpy.SetProgressorLabel("Writing to log file in {}".format(eventFC))
            WriteLogFile(txtFile, eventFC)

        # Close the log file
        txtFile.close()

        return

def GetFeatureDataset(FC):
    fcPath = arcpy.Describe(FC).catalogPath
    fcHome = os.path.dirname(fcPath)

    if arcpy.Describe(fcHome).dataType == "FeatureDataset":
        return fcHome
    else:
        return None

def PopulateEventsDict(eventFCList):
    for eventFC in eventFCList:
        eventFCDict[eventFC] = IsPointEvent(eventFC)

def ValidateIntersectionParam(intersectionParam, isNewIntersectionFC):
    # IntersectionFC is the whole path to the feature class in the GDB.
    intersectionFC = intersectionParam.valueAsText
    intersectionFCName = intersectionFC.split("\\")[-1];

    #Ensure the parameter is a Feature Class
    if arcpy.Describe(intersectionFC).dataType != "FeatureClass":
        intersectionParam.setErrorMessage("The parameter value is not a feature class.")

    # Check if Intersection is in LRS Dataset.
    isLRSInt = False
    lrsNetworkDataset = GetFeatureDataset(intersectionFC)
    if not lrsNetworkDataset:
        intersectionParam.setErrorMessage("The parameter value is not a valid LRS intersection.")
        return

    networkDatasetName = lrsNetworkDataset.split('\\')[-1]
    desc = arcpy.Describe(lrsNetworkDataset + '\\' + networkDatasetName)
    lrsMetadata = desc.LrsMetadata

    if not lrsMetadata:
        arcpy.AddError("Cannot find LRS")

    # Get LRS name from network
    root = ET.fromstring(lrsMetadata)
    rootIntersections = root.iter('IntersectionClass')

    userSchemaPrefix = root.get('UserSchemaPrefix')
    intersectionFCName = intersectionFCName.replace(userSchemaPrefix, "")

    # Find input intersection in LRS
    for intersection in rootIntersections:
        intName = intersection.get('Name')
        if intName == intersectionFCName:
            isLRSInt = True
            intType = intersection.get('NewIntersectionsFormat')
            if isNewIntersectionFC and intType == 'false':
                intersectionParam.setErrorMessage("The parameter value is not a valid Pro LRS intersection.")
                return
            elif not isNewIntersectionFC and intType == 'true':
                intersectionParam.setErrorMessage("The parameter value is not a valid ArcMap LRS intersection.")
                return
            break

    # Intersection not registered with LRS.
    if not isLRSInt:
        intersectionParam.setErrorMessage("The parameter value is not a valid LRS intersection.")

def ValidateParameters(parameters):
    
    #region Old Intersection Validation
    
    if parameters[0].valueAsText: 
        ValidateIntersectionParam(parameters[0], False)

    #endregion

    #region New Intersection Validation

    if parameters[1].valueAsText: 
        # Check if Old Intersection is in LRS Dataset.
        ValidateIntersectionParam(parameters[1], True)
        
    #endregion

    #region Event Validation
    if parameters[2].valueAsText: 
        eventFC = parameters[2].valueAsText
        #eventFCName = eventFC.split("\\")[-1];
        eventFCList = eventFC.split(";")
        # Check if Event is in LRS Dataset.
        for eventFC in eventFCList:
            #Ensure the parameter is a Feature Class
            if arcpy.Describe(eventFC).dataType != "FeatureClass":
                parameters[2].setErrorMessage("The parameter value is not a feature class.")

            isLRSEvent = False
            lrsNetworkDataset = GetFeatureDataset(eventFC)
            if not lrsNetworkDataset:
                parameters[2].setErrorMessage("The parameter value is not a valid LRS event.")
                break

            networkDatasetName = lrsNetworkDataset.split('\\')[-1]
            desc = arcpy.Describe(lrsNetworkDataset + '\\' + networkDatasetName)
            lrsMetadata = desc.LrsMetadata
            if not lrsMetadata:
                arcpy.AddError("Cannot find LRS")

            # Get LRS name from network
            root = ET.fromstring(lrsMetadata)
            rootEvents = root.iter('EventTable')

            userSchemaPrefix = root.get('UserSchemaPrefix')
            eventFCName = eventFC.split("\\")[-1];
            eventFCName = eventFCName.replace(userSchemaPrefix, "")
            
            # Find input event in LRS
            for ev in rootEvents:
                eventName = ev.get('Name')
                if eventName == eventFCName:
                    isLRSEvent = True
                    break

            # Event not registered with LRS.
            if not isLRSEvent:
                parameters[2].setErrorMessage("The parameter value is not a valid LRS event.")

    #endregion

    return

class PointEvent(object):
    def __init__(self, id, old_ref_location, ref_location, from_date, to_date):
        self.id = id
        self.old_ref_location = old_ref_location
        self.ref_location = ref_location
        self.from_date = from_date
        self.to_date = to_date

    ref_location_updated = False

class LineEvent(object):
    def __init__(self, id, old_ref_location, ref_location, old_toref_location, toref_location, from_date, to_date):
        self.id = id
        self.old_ref_location = old_ref_location
        self.ref_location = ref_location
        self.old_toref_location = old_toref_location
        self.toref_location = toref_location
        self.from_date = from_date
        self.to_date = to_date

    ref_location_updated = False
    toref_location_updated = False

# Variables
coded_value_old_intersection = -1
coded_value_new_intersection = -1
events_with_intersections = []
tmp_spatial_join_output = "tmpSpatialJoinOutput"
do_not_update = "DO_NOT_UPDATE"
eventFCDict = {}

#determine if events are points or lines
def IsPointEvent(eventFC):
    events_lyr = "events"
    arcpy.MakeFeatureLayer_management(eventFC, events_lyr)
    desc = arcpy.Describe(events_lyr)
    geometryType = desc.shapeType
    return geometryType == 'Point'

# convert the dReferentMethod domain to a table and query the table for description matching old intersection name
def SetCodedValueDomains(parameters):
    global coded_value_old_intersection
    global coded_value_new_intersection
    tmp_domain_table = "tmpDomainTable"

    oldIntersectionFC = parameters[0].valueAsText
    newIntersectionFC = parameters[1].valueAsText
    eventFC = parameters[2].valueAsText
    eventFCList = eventFC.split(";")

    old_intersection_name = os.path.basename(oldIntersectionFC)
    new_intersection_name = os.path.basename(newIntersectionFC)

    fcPath = arcpy.Describe(eventFCList[0]).catalogPath
    fcHome = os.path.dirname(fcPath)
    gdbHome = os.path.dirname(fcHome)

    arcpy.env.workspace = gdbHome

    #Update the old_intersection_name
    lrsNetworkDataset = GetFeatureDataset(newIntersectionFC)

    networkDatasetName = lrsNetworkDataset.split('\\')[-1]
    desc = arcpy.Describe(lrsNetworkDataset + '\\' + networkDatasetName)
    lrsMetadata = desc.LrsMetadata
    if not lrsMetadata:
        arcpy.AddError("Cannot find LRS")

    # Get LRS name from network
    root = ET.fromstring(lrsMetadata)
    userSchemaPrefix = root.get('UserSchemaPrefix')
    
    old_intersection_name = old_intersection_name.replace(userSchemaPrefix, "")
    new_intersection_name = new_intersection_name.replace(userSchemaPrefix, "")

    # Validate the event's RefMethod fields and ensure that they are all using the same domain
    domain_name = ""
    for event in eventFCList:
        # Find refMethod field name in event fc
        refFieldNames = FindReferentFieldsForEvent(event)

        if refFieldNames["FromRefMethod"] == "":
            # RefMethod was not found
            arcpy.AddError("The RefMethod domain was not found in {}".format(event))
            return False

        for field in arcpy.ListFields(event, refFieldNames["FromRefMethod"]):
            if domain_name == "":
                domain_name = field.domain
                # domain will be the same name across all events so we can break after setting the first one
                break
        
        if domain_name == "":
            # domain for RefMethod was not found
            arcpy.AddError("The RefMethod domain was not found")
            return False

    arcpy.management.DomainToTable(arcpy.env.workspace, domain_name, tmp_domain_table, "CodedValue", "CodedValueDescription", '')

    fields = ["CodedValue", "CodedValueDescription"]
    with arcpy.da.SearchCursor(tmp_domain_table, fields) as cursor:
        for row in cursor:
            if row[1] == old_intersection_name:
                coded_value_old_intersection = row[0]
            elif row[1] == new_intersection_name:
                coded_value_new_intersection = row[0]

            if coded_value_old_intersection != -1 and coded_value_new_intersection != -1:
                break

    arcpy.management.Delete(tmp_domain_table)

    return True

def FindReferentFieldsForEvent(eventFC):
    eventFCName = eventFC.split("\\")[-1]
    fromRefMethodFieldName = ""
    toRefMethodFieldName = ""
    fromRefLocationFieldName = ""
    toRefLocationFieldName = ""

    refFieldNames = {"FromRefMethod": fromRefMethodFieldName,
                     "ToRefMethod": toRefMethodFieldName,
                     "FromRefLocation": fromRefLocationFieldName,
                     "ToRefLocation": toRefLocationFieldName}

    pointEvent = eventFCDict[eventFC]

    # Find refMethod field name in event fc
    lrsNetworkDataset = GetFeatureDataset(eventFC)
    networkDatasetName = lrsNetworkDataset.split('\\')[-1]
    desc = arcpy.Describe(lrsNetworkDataset + '\\' + networkDatasetName)
    lrsMetadata = desc.LrsMetadata

    # Get LRS name from network
    root = ET.fromstring(lrsMetadata)

    userSchemaPrefix = root.get('UserSchemaPrefix')
    eventFCName = eventFC.split("\\")[-1];
    eventFCName = eventFCName.replace(userSchemaPrefix, "")

    rootEvents = root.iter('EventTable')
    for ev in rootEvents:
        eventName = ev.get('Name')
        if eventName == eventFCName:
            refFieldNames["FromRefMethod"] = ev.get('FromReferentMethodFieldName')
            refFieldNames["FromRefLocation"] = ev.get('FromReferentLocationFieldName')
            if not pointEvent:
                refFieldNames["ToRefMethod"] = ev.get('ToReferentMethodFieldName')
                refFieldNames["ToRefLocation"] = ev.get('ToReferentLocationFieldName')
            break

    return refFieldNames

def UpdateEventsWithIntersections(eventFC):

    # Find refMethod field name in event fc
    refFieldNames = FindReferentFieldsForEvent(eventFC)
    if eventFCDict[eventFC]:
        fields = ["OBJECTID", refFieldNames["FromRefMethod"], refFieldNames["FromRefLocation"], "FromDate", "ToDate"]
        with arcpy.da.SearchCursor(eventFC, fields) as cursor:
            for row in cursor:
                # coded value corresponds to old intersection name
                if row[1] == coded_value_old_intersection:
                    events_with_intersections.append(PointEvent(row[0], row[2], row[2], row[3], row[4]))
    else:
        fields = ["OBJECTID", refFieldNames["FromRefMethod"], refFieldNames["FromRefLocation"], 
                  refFieldNames["ToRefMethod"], refFieldNames["ToRefLocation"], "FromDate", "ToDate" ]
        with arcpy.da.SearchCursor(eventFC, fields) as cursor:
            for row in cursor:
                # coded value corresponds to old intersection name
                if row[1] == coded_value_old_intersection and row[3] == coded_value_old_intersection:
                    events_with_intersections.append(LineEvent(row[0], row[2], row[2], row[4], row[4], row[5], row[6]))
                elif row[1] == coded_value_old_intersection and row[3] != coded_value_old_intersection:
                    events_with_intersections.append(LineEvent(row[0], row[2], row[2], do_not_update, do_not_update, row[5], row[6]))
                elif row[1] != coded_value_old_intersection and row[3] == coded_value_old_intersection:
                    events_with_intersections.append(LineEvent(row[0], do_not_update, do_not_update, row[4], row[4], row[5], row[6] ))

# - Perform a spatial join using the new intersections and the old intersections.
def PerformSpatialJoinAnalysis(parameters, eventFC):
    pointEvent = eventFCDict[eventFC]
    
    oldIntersectionFC = parameters[0].valueAsText
    newIntersectionFC = parameters[1].valueAsText

    if arcpy.Exists(tmp_spatial_join_output):
        arcpy.management.Delete(tmp_spatial_join_output)

    # get the tolerance used in SpatialJoin
    sr = arcpy.Describe(eventFC).spatialReference
    tolerance = sr.XYTolerance * sr.metersPerUnit
    toleranceString = "{} Meters".format(tolerance)

    arcpy.analysis.SpatialJoin(newIntersectionFC, oldIntersectionFC, tmp_spatial_join_output,
                               "JOIN_ONE_TO_MANY", "KEEP_COMMON",
                               'IntersectionId "IntersectionId" true false false 38 Guid 0 0,First,#,{},IntersectionId,-1,-1,{},INTERSECTIONID,-1,-1;'.format(newIntersectionFC, oldIntersectionFC) +
                               'IntersectionName "IntersectionName" true true false 1000 Text 0 0,First,#,{},IntersectionName,0,1000,{},INTERSECTIONNAME,0,255;'.format(newIntersectionFC, oldIntersectionFC) +
                               'RouteId "RouteId" true false false 1000 Text 0 0,First,#,{},RouteId,0,1000,{},ROUTEID,0,255;'.format(newIntersectionFC, oldIntersectionFC) +
                               'FeatureId "FeatureId" true true false 1000 Text 0 0,First,#,{},FeatureId,0,1000,{},FEATUREID,0,100;'.format(newIntersectionFC, oldIntersectionFC) +
                               'FeatureClassName "FeatureClassName" true false false 150 Text 0 0,First,#,{},FeatureClassName,0,150,{},FEATURECLASSNAME,0,150;'.format(newIntersectionFC, oldIntersectionFC) +
                               'FromDate "FromDate" true true false 8 Date 0 0,First,#,{},FromDate,-1,-1,{},FROMDATE,-1,-1;'.format(newIntersectionFC, oldIntersectionFC) +
                               'ToDate "ToDate" true true false 8 Date 0 0,First,#,{},ToDate,-1,-1,{},TODATE,-1,-1;'.format(newIntersectionFC, oldIntersectionFC) +
                               'Measure "Measure" true true false 8 Double 0 0,First,#,{},Measure,-1,-1;'.format(newIntersectionFC) +
                               'OldIntersectionId "OldIntersectionId" true true false 255 Text 0 0,First,#,{},INTERSECTIONID,-1,-1'.format(oldIntersectionFC), "INTERSECT", None, '')
    

    # OldIntersectionId = old intersection ID
    # IntersectionId = new intersection ID
    fields = ["OldIntersectionId", "IntersectionId", "FromDate", "ToDate"]
    with arcpy.da.SearchCursor(tmp_spatial_join_output, fields) as cursor:
        for row in cursor:
            intersect_fromdt = row[2]
            intersect_todt = row[3]
            for event in events_with_intersections:
                # Only update event if intersection is within event date range
                if intersect_fromdt <= event.from_date and (intersect_todt is None or intersect_todt > event.from_date):
                    if row[0] == event.ref_location and not event.ref_location_updated:
                        event.old_ref_location = row[0]
                        event.ref_location = row[1]
                        event.ref_location_updated = True
                    if not pointEvent and row[0] == event.toref_location and not event.toref_location_updated:
                        event.old_toref_location = row[0]
                        event.toref_location = row[1]
                        event.toref_location_updated = True

    # Find refMethod field name in event fc
    refFieldNames = FindReferentFieldsForEvent(eventFC)

    # - Update event features with their associated new RefMethod and RefLocation
    if pointEvent:
        fields = ["OBJECTID", refFieldNames["FromRefMethod"], refFieldNames["FromRefLocation"]]
        with arcpy.da.UpdateCursor(eventFC, fields) as cursor:
            for row in cursor:
                for event in events_with_intersections:
                    if row[0] == event.id and event.ref_location_updated:
                        # Update the RefMethod with new intersection fc
                        row[1] = coded_value_new_intersection
                        # Update the RefLocation with new intersection id
                        row[2] = event.ref_location
                        cursor.updateRow(row)
    else:
        fields = ["OBJECTID", refFieldNames["FromRefMethod"], refFieldNames["FromRefLocation"], 
                  refFieldNames["ToRefMethod"], refFieldNames["ToRefLocation"] ]
        with arcpy.da.UpdateCursor(eventFC, fields) as cursor:
            for row in cursor:
                for event in events_with_intersections:
                    if row[0] == event.id:
                        if row[1] == coded_value_old_intersection and event.ref_location_updated:
                            # Update the RefMethod with new intersection fc
                            row[1] = coded_value_new_intersection
                            # Update the RefLocation with new intersection id                     
                            row[2] = event.ref_location
                
                        if row[3] == coded_value_old_intersection and event.toref_location_updated:
                            # Update the ToRefMethod with new intersection fc
                            row[3] = coded_value_new_intersection
                            # Update the ToRefLocation with new intersection id
                            row[4] = event.toref_location

                        cursor.updateRow(row)

    # - Remove tmp feature class
    if arcpy.Exists(tmp_spatial_join_output):
        arcpy.management.Delete(tmp_spatial_join_output)

def OpenLogFile():
    outputDir = arcpy.env.scratchFolder
    outputLogName = 'output' + r".log"
    outputFileLog = os.path.join(outputDir, outputLogName)

    if arcpy.Exists(outputFileLog):
        arcpy.management.Delete(outputFileLog)

    arcpy.AddMessage("Successfully ran against event features, see output log for more information:\n")
    arcpy.AddMessage(outputFileLog)
    txtFile = open(outputFileLog, 'w')

    return txtFile

def WriteLogFile(txtFile, eventFC):

    updatedEventsOutput = ''
    nonupdatedEventsOutput = ''
    updatedCount = 0
    errorCount = 0
    if eventFCDict[eventFC]:
        for event in events_with_intersections:
            if event.ref_location_updated:
                updatedEventsOutput += 'OID: {}, previous RefLocation: {}, new RefLocation: {} \n'.format(event.id, event.old_ref_location, event.ref_location)
            else:
                nonupdatedEventsOutput += '{}, '.format(event.id)

        updatedCount = sum(event.ref_location_updated == True for event in events_with_intersections)
        errorCount = sum(event.ref_location_updated == False for event in events_with_intersections)
    else:
        for event in events_with_intersections:
           if event.ref_location_updated and event.toref_location_updated:
                updatedEventsOutput += 'OID: {}, previous From RefLocation: {}, new From RefLocation: {}, previous To RefLocation: {}, new To RefLocation: {} \n'.format(event.id, event.old_ref_location, event.ref_location, event.old_toref_location, event.toref_location)
           elif event.ref_location_updated:
                updatedEventsOutput += 'OID: {}, previous From RefLocation: {}, new From RefLocation: {} \n'.format(event.id, event.old_ref_location, event.ref_location)
                if event.toref_location != do_not_update:
                    nonupdatedEventsOutput += '{}, '.format(event.id)
           elif event.toref_location_updated:
                updatedEventsOutput += 'OID: {}, previous To RefLocation: {}, new To RefLocation: {} \n'.format(event.id, event.old_toref_location, event.toref_location)
                if event.ref_location != do_not_update:
                    nonupdatedEventsOutput += '{}, '.format(event.id)
           elif event.ref_location != do_not_update or event.toref_location != do_not_update:
               nonupdatedEventsOutput += '{}, '.format(event.id)

        updatedCount = sum((event.ref_location_updated == True or event.ref_location == do_not_update) and 
                           (event.toref_location_updated == True or event.toref_location == do_not_update) for event in events_with_intersections)
        errorCount = sum((event.ref_location_updated == False and event.ref_location != do_not_update) or 
                         (event.toref_location_updated == False and event.toref_location != do_not_update) for event in events_with_intersections)

    if updatedCount > 0:
        txtFile.write('{}: Intersection referents updated\n'.format(eventFC))
        txtFile.write(updatedEventsOutput)
        txtFile.write('\n\n')
    else:
        txtFile.write('{}: No intersection referents updated\n\n'.format(eventFC))

    if errorCount > 0:
        txtFile.write('{}: OID(s) of event(s) that were not processed because new referent location could not be found\n'.format(eventFC))
        txtFile.write(nonupdatedEventsOutput[:-2])
        txtFile.write('\n\n')
    else:
        txtFile.write('{}: No errors found\n\n'.format(eventFC))

    





