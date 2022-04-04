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
import arcpy, uuid, os, logging
import xml.etree.ElementTree as ET
import math, datetime
from arcpy import env
from typing import NamedTuple


class MigrateCalibrationPoints(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Migrate Calibration Points"
        self.description = "Routes that contain looped section require a minimum of two calibration points within the loop in order to remain calibrated. This script will add the minimum number of calibration points to keep the route calibrated. If the loop already contains at least two calibration points, no changes will be made. The new calibration points will not affect existing calibration or apply any event behaviors."
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        # Network Parameter
        params = []
        param0 = arcpy.Parameter(
            displayName="Network Feature Class",
            name="in_network_features",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Input")

        param0.filter.list = ["Polyline"]
        params.append(param0)

        # Calibration Point Parameter
        param1 = arcpy.Parameter(
            displayName="Calibration Point Feature Class",
            name="in_calibration_point_features",
            datatype="DEFeatureClass",
            parameterType="Required",
            direction="Input")

        param1.filter.list = ["Point"]
        params.append(param1)

        # Derived Output Parameter
        param2 = arcpy.Parameter(
            displayName="Output Calibration Point Features",
            name="out_calibration_point_features",
            datatype="DEFeatureClass",
            parameterType="Derived",
            direction="Output")

        param2.parameterDependencies = [param1.name]
        param2.schema.clone = True
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

        # Get Parameter info.
        inputNetworkName = parameters[0].valueAsText
        inputNetworkValue = parameters[0].value

        inputCalibrationPointName = parameters[1].valueAsText
        inputCalibrationPointValue = parameters[1].value

        # Validate Network Feature
        if parameters[0].altered:

            if IsFeatureLayer(inputNetworkName, parameters[0]):
                return

            lrsMetadata = GetLrsMetadata(inputNetworkName, True, parameters[0])
            if lrsMetadata is None:
                return

            network = GetNetworkFromMetadata(lrsMetadata, inputNetworkName, True, parameters[0])
            if network is None:
                return

        # Validate Calibration Point Feature
        if parameters[1].altered:

            if IsFeatureLayer(inputCalibrationPointName, parameters[1]):
                return

            lrsMetadata = GetLrsMetadata(inputCalibrationPointName, True, parameters[1])
            if lrsMetadata is None:
                return

            calibrationPoint = GetCalibrationPointFromMetadata(lrsMetadata, inputCalibrationPointName, True,
                                                               parameters[1])
            if calibrationPoint is None:
                return

        # Check if both inputs are LRS features and in a LRS dataset
        if inputNetworkName and inputCalibrationPointName:
            networkParentDataset = GetFeatureDataset(inputNetworkName)
            calibrationPointParentDataset = GetFeatureDataset(inputCalibrationPointName)

            if (networkParentDataset != calibrationPointParentDataset):
                parameters[0].setErrorMessage("Input Network and Calibration Point features are in different datasets")
                return

            networkLrsMetadata = GetLrsMetadata(inputNetworkName, True, parameters[0])
            calibrationPointLrsMetadata = GetLrsMetadata(inputCalibrationPointName, True, parameters[1])

            root = ET.fromstring(networkLrsMetadata)
            NetworkLRSName = root.attrib['Name']

            root = ET.fromstring(calibrationPointLrsMetadata)
            calibrationPointLRSName = root.attrib['Name']

            if (NetworkLRSName != calibrationPointLRSName):
                parameters[0].setErrorMessage(
                    "Input Network and Calibration Point features are not registered with same LRS")
                return

        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        # Network Info
        networkFC = parameters[0].valueAsText
        networkPath = arcpy.Describe(networkFC).catalogPath
        lrsMetaData = GetLrsMetadata(networkFC)
        networkMetaData = GetNetworkFromMetadata(lrsMetaData, networkFC)
        networkField = GetNetworkFields(networkMetaData)
        networkId = networkField.NetworkId

        # Calibration Point Info
        calibrationPointFC = parameters[1].valueAsText
        calibrationPointPath = arcpy.Describe(calibrationPointFC).catalogPath
        calibrationPointMetadata = GetLrsMetadata(calibrationPointFC)
        calibrationPointFields = GetCalibrationPointFields(calibrationPointMetadata)

        # Workspace Info
        workspace = os.path.dirname(os.path.dirname(calibrationPointPath))
        tolerances = GetTolerances(networkFC)

        # Start edit session.
        edit = StartEditSession(workspace)

        nanOids = []
        nonMonotonicOids = []
        CheckForInvalidRoutes(networkPath, networkField, nanOids, nonMonotonicOids)

        editedRouteOids = []
        editedCpOids = []

        # Get only routes that contain loops.
        routesWithLoops = GetRoutesWithLoops(networkPath, networkField, tolerances, nanOids, nonMonotonicOids)

        # If loops are present.
        if routesWithLoops:
            # Check if loops have existing intermediate cps.
            intermediateCpsInLoops = GetExistingIntermediateCps(calibrationPointPath, calibrationPointFields,
                                                                tolerances, routesWithLoops, nanOids)

            # Find new cps that need to be added.
            cpRecordsToAdd = GetCpRecordsToAdd(calibrationPointPath, tolerances, routesWithLoops,
                                                intermediateCpsInLoops)

            # Write new records to Calibration Point Feature class.
            editedRouteOids = WriteToFeature(calibrationPointPath, calibrationPointFields, workspace,
                                                cpRecordsToAdd)

        # Check for cps with incorrect Z values.
        editedCps = GetAdjustZValuesForCalibrationPoints(networkPath, networkField, calibrationPointPath,
                                                            calibrationPointFields, tolerances, nanOids)

        # Update calibration point if needed.
        if editedCps:
            editedCpOids = UpdateCalibrationRecords(calibrationPointPath, editedCps, workspace)

        # Stop edit session.
        StopEditSession(edit)

        # Write to output log file if any new cps were added/updated.
        WriteLogFile(editedRouteOids, editedCpOids, nanOids, nonMonotonicOids)

        return


##******************************************##
##      Looped Routes Functions             ##
##******************************************##

def CheckForInvalidRoutes(networkFC, fields, nanOids, nonMonotoicOids):
    # Set progressor bar and label.
    featureCount = int(arcpy.GetCount_management(networkFC)[0])
    arcpy.SetProgressor("step", "Validating Routes...",
                        0, featureCount, 1)

    for row in arcpy.da.SearchCursor(networkFC, ["OID@", fields.RouteId, fields.FromDate, fields.ToDate, "SHAPE@"]):

        # Store row in tuple for readability.
        rowValues = RouteInfo(Oid=row[0], RouteId=row[1], FromDate=row[2], ToDate=row[3], FromM=0, ToM=0, Network=0,
                              Geometry=row[4])

        # Skip null geometry
        if rowValues.Geometry is None:
            arcpy.SetProgressorPosition()
            continue

        Invalid = False
        currentValue = None;
        for part in rowValues.Geometry:
            if Invalid == True:
                break

            for point in part:
                if (math.isnan(point.M)):
                    nanOids.append(row[0])
                    Invalid = True
                    break
                elif (currentValue is not None and point.M < currentValue):
                    nonMonotoicOids.append(row[0])
                    Invalid = True;
                    break;
                currentValue = point.M

        arcpy.SetProgressorPosition()

    return nanOids


def GetRoutesWithLoops(networkFC, fields, tolerances, nanOids, nonMonotonicOids):
    # Set progressor bar and label.
    featureCount = int(arcpy.GetCount_management(networkFC)[0])
    arcpy.SetProgressor("step", "Finding Routes with Loops...",
                        0, featureCount, 1)

    # Iterate all routes in Network.
    routeInfo = {}
    for row in arcpy.da.SearchCursor(networkFC, ["OID@", fields.RouteId, fields.FromDate, fields.ToDate, "SHAPE@"]):

        # Store row in tuple for readability.
        rowValues = RouteInfo(Oid=row[0], RouteId=row[1], FromDate=row[2], ToDate=row[3], FromM=0, ToM=0, Network=0,
                              Geometry=row[4])

        # Skip null geometry
        if rowValues.Geometry is None:
            arcpy.SetProgressorPosition()
            continue

        # Skip routes with nan values
        if row[0] in nanOids:
            continue

        if row[0] in nonMonotonicOids:
            continue

        pointIndex = []
        for part in rowValues.Geometry:

            for point in part:

                # Empty list, add first point.
                if not pointIndex:
                    pointIndex.append((point.X, point.Y, point.Z, point.M))

                # Check if point exist in list.
                else:
                    loopFound = False

                    # Iterate list looking for point. Matches should have same
                    # X, Y, Z location but different M values.
                    for index in pointIndex:

                        if (not math.isnan(index[3]) and
                                not math.isnan(point.M) and
                                not math.isclose(index[3], point.M, rel_tol=tolerances.MTolerance,
                                                 abs_tol=tolerances.MTolerance) and
                                math.isclose(index[0], point.X, abs_tol=tolerances.XYTolerance) and
                                math.isclose(index[1], point.Y, abs_tol=tolerances.XYTolerance) and
                                math.isclose(index[2], point.Z, abs_tol=tolerances.ZTolerance)):

                            alreadyFound = False

                            # Found duplicate vertex. Store info with the start/end measure of the loop.
                            info = RouteInfo(rowValues.Oid, rowValues.RouteId, rowValues.FromDate, rowValues.ToDate,
                                             index[3], point.M, fields.NetworkId, rowValues.Geometry)

                            if rowValues.RouteId not in routeInfo:
                                routeInfo[rowValues.RouteId] = []
                            else:

                                # Make sure we don't add duplicates.
                                for route in routeInfo[rowValues.RouteId]:
                                    value = RouteInfo(**route)

                                    if (info.RouteId == value.RouteId and
                                            info.FromDate == value.FromDate and
                                            info.ToDate == value.ToDate and
                                            info.FromM == value.FromM and
                                            info.ToM == value.ToM):
                                        alreadyFound = True

                            if not alreadyFound:
                                # Loop detected.
                                routeInfo[rowValues.RouteId].append(info._asdict())
                                loopFound = True
                                break

                    if not loopFound:
                        # No matching vertices's.
                        pointIndex.append((point.X, point.Y, point.Z, point.M))

        arcpy.SetProgressorPosition()

    return routeInfo


def GetExistingIntermediateCps(calibrationPointFC, fields, tolerances, loopedRoutes, nanOids):
    # Set progressor label.
    arcpy.SetProgressorLabel("Finding existing intermediate cps...")

    # Get route ids and set where clause.
    routeIds = loopedRoutes.keys()
    valueList = ["'%s'" % value for value in routeIds]
    whereclause = '%s IN (%s)' % (fields.RouteId, ','.join(map(str, valueList)))

    # Search for calibration points for looped routes.
    existingLoopCps = {}
    for row in arcpy.da.SearchCursor(calibrationPointFC,
                                     [fields.RouteId, fields.FromDate, fields.ToDate, fields.Measure, "SHAPE@"],
                                     where_clause=whereclause):

        # Store row in tuple for readability.
        rowValues = RouteInfo(Oid=0, RouteId=row[0], FromDate=row[1], ToDate=row[2], FromM=row[3], ToM=row[3],
                              Network=0, Geometry=row[4])

        # Skip calibration points with nan values.
        if rowValues.FromM is None or math.isnan(rowValues.FromM):
            continue

        if rowValues.ToM is None or math.isnan(rowValues.ToM):
            continue

        for value in loopedRoutes[rowValues.RouteId]:
            routeInfo = RouteInfo(**value)

            # Check if calibration point is within time span of route and
            # if its measure is between the start and end of the loop.
            if (Intersects(rowValues.FromDate, routeInfo.FromDate, rowValues.ToDate, routeInfo.ToDate) and
                    not math.isclose(rowValues.FromM, routeInfo.FromM, rel_tol=tolerances.MTolerance,
                                     abs_tol=tolerances.MTolerance) and
                    not math.isclose(rowValues.FromM, routeInfo.ToM, rel_tol=tolerances.MTolerance,
                                     abs_tol=tolerances.MTolerance) and
                    rowValues.FromM > routeInfo.FromM and rowValues.FromM < routeInfo.ToM):

                # Found intermediate cp.
                if routeInfo.Oid not in existingLoopCps:
                    existingLoopCps[routeInfo.Oid] = []

                existingLoopCps[routeInfo.Oid].append(
                    RouteInfo(routeInfo.Oid, routeInfo.RouteId, routeInfo.FromDate, routeInfo.ToDate, rowValues.FromM,
                              rowValues.FromM, routeInfo.Network, rowValues.Geometry))

    return existingLoopCps


def GetCpRecordsToAdd(calibrationPointFC, tolerances, loopedRoutes, cpsInLoops):
    # Set progressor label.
    arcpy.SetProgressorLabel("Creating intermediate loop cps...")

    # Compare routes that contain loops with existing intermediate cps.
    recordsToAdd = []
    for key, routes in loopedRoutes.items():
        newRecords = []
        for route in routes:
            routeInfo = RouteInfo(**route)

            # Get existing cps associated with this routes oid.
            existingCps = []
            if routeInfo.Oid in cpsInLoops.keys():
                for existingCp in cpsInLoops[routeInfo.Oid]:
                    existingCps.append(existingCp)

            # Check if this route has existing cps in loop.
            if len(existingCps) == 1:

                # Get length of the loop.
                loopLength = routeInfo[5] - routeInfo[4]

                # Only add if there is one cp.
                if len(cpsInLoops[routeInfo.Oid]) == 1:
                    midPoint = (loopLength / 2) + routeInfo[4]
                    existingMeasure = existingCps[0].FromM

                    # Add to the second half of the loop.
                    if midPoint > existingMeasure:
                        newvalue = (routeInfo.ToM + existingMeasure) / 2
                        newPoint = GetPoint(routeInfo, newvalue, tolerances)

                    # Add to the first half of the loop.
                    else:
                        newvalue = (existingMeasure + routeInfo.FromM) / 2
                        newPoint = GetPoint(routeInfo, newvalue, tolerances)

                    newRecords.append(
                        RouteInfo(routeInfo.Oid, routeInfo.RouteId, routeInfo.FromDate, routeInfo.ToDate, newvalue,
                                  newvalue, routeInfo.Network, newPoint))

            # No intermediate cps in loop. Add 2 cps.
            elif len(existingCps) == 0:

                onethird = ((routeInfo.ToM - routeInfo.FromM) / 3) + routeInfo.FromM
                twothrid = ((routeInfo.ToM - routeInfo.FromM) / 3) + (
                            (routeInfo.ToM - routeInfo.FromM) / 3) + routeInfo.FromM

                newPoint1 = GetPoint(routeInfo, onethird, tolerances)
                newPoint2 = GetPoint(routeInfo, twothrid, tolerances)

                newRecords.append(
                    RouteInfo(routeInfo.Oid, routeInfo.RouteId, routeInfo.FromDate, routeInfo.ToDate, onethird,
                              onethird, routeInfo.Network, newPoint1))
                newRecords.append(
                    RouteInfo(routeInfo.Oid, routeInfo.RouteId, routeInfo.FromDate, routeInfo.ToDate, twothrid,
                              twothrid, routeInfo.Network, newPoint2))

        # No need simplify for timeslices.
        if len(newRecords) == 1:
            recordsToAdd.append(newRecords[0])

        # Check if we can reduce records for timeslices.
        elif len(newRecords) > 1:
            adjustedRecords = AdjustForTimeslices(newRecords, tolerances)

            for record in adjustedRecords:
                recordsToAdd.append(record)

    return recordsToAdd


def AdjustForTimeslices(records, tolerances):
    # Sorter
    def sortFunc(e):
        return e[2]

    # Sort by from date.
    records.sort(key=sortFunc)

    # Compare each record to see if the location and measure are the same.
    # If they are, reduce the two records to one, with date covering both timeslices.
    for i in range(len(records)):
        tempi = records[i]

        if tempi.Oid != -1:

            for j in range(i + 1, len(records)):
                tempj = records[j]

                if tempj.Oid != -1:

                    # Check for records at same location with same measure.
                    if (math.isclose(tempi.FromM, tempj.FromM, rel_tol=tolerances.MTolerance,
                                     abs_tol=tolerances.MTolerance) and
                            math.isclose(tempi.Geometry.X, tempj.Geometry.X, abs_tol=tolerances.XYTolerance) and
                            math.isclose(tempi.Geometry.Y, tempj.Geometry.Y, abs_tol=tolerances.XYTolerance) and
                            math.isclose(tempi.Geometry.Z, tempj.Geometry.Z, abs_tol=tolerances.ZTolerance)):

                        # Check if we can merge records based on there time span. Since the
                        # records are sorted by from date, a single pass should do.

                        # Duplicate record. Invalidate record j.
                        if (CompareDate(tempi.FromDate, tempj.FromDate, False, False) and
                                CompareDate(tempi.ToDate, tempj.ToDate, False, False)):
                            records[j] = records[j]._replace(Oid=-1)

                        # Shouldn't happen due to sort, but just in case.
                        # ex: tempj == (1/1/2000 - 1/1/2010) tempi == (1/1/2010 - null).
                        # tempj.FromDate < tempi.FromDate and tempj.ToDate == tempi.FromDate.
                        # tempi new dates will be (1/1/2000 - null).
                        if (CompareDate(tempj.FromDate, tempi.FromDate, True, False) and
                                CompareDate(tempj.ToDate, tempi.FromDate, False, False)):
                            records[i] = records[i]._replace(FromDate=tempj.FromDate)
                            records[j] = records[j]._replace(Oid=-1)

                        # ex: tempj == (1/1/2010 - null) tempi == (1/1/2000 - 1/1/2010).
                        # tempj.ToDate > tempi.FromDate and tempj.FromDate == tempi.ToDate.
                        # tempi new dates will be (1/1/2000 - null).
                        if (CompareDate(tempj.ToDate, tempi.ToDate, False, True) and
                                CompareDate(tempj.FromDate, tempi.ToDate, False, False)):
                            records[i] = records[i]._replace(ToDate=tempj.ToDate)
                            records[j] = records[j]._replace(Oid=-1)

    # Remove records that were merged.
    for record in records:
        if record.Oid == -1:
            records.remove(record)

    return records


def WriteToFeature(calibrationPointFC, fields, workspace, recordsToAdd):
    # Set progressor label.
    arcpy.SetProgressorLabel("Saving new calibration points...")

    editedRouteOid = []
    if recordsToAdd:
        try:

            formatedRecord = {}

            # We will need to create blank records for our new records first. If we insert values
            # directly, the Oid value will be set to -1 when it reaches the controller dataset. This
            # is seen as an error by the controller dataset. Creating a blank record generates a Oid
            # that we will update.
            with arcpy.da.InsertCursor(calibrationPointFC, ["OID@"]) as insertCursor:

                for row in recordsToAdd:

                    # Add edited route oids only once.
                    if row.Oid not in editedRouteOid:
                        editedRouteOid.append(row.Oid)

                    oid = insertCursor.insertRow([None])
                    formatedRecord[oid] = (
                    [oid, row.RouteId, row.FromDate, row.ToDate, row.FromM, row.Network, row.Geometry])

            # Where clause using Oids we got from above.
            valueList = formatedRecord.keys()
            whereclause = '%s IN (%s)' % ('OBJECTID', ','.join(map(str, valueList)))

            # Update the records with the new values.
            with arcpy.da.UpdateCursor(calibrationPointFC,
                                       ["OID@", fields.RouteId, fields.FromDate, fields.ToDate, fields.Measure,
                                        fields.NetworkId, "SHAPE@"], whereclause) as updateCursor:

                for row in updateCursor:
                    row[1] = formatedRecord[row[0]][1]
                    row[2] = formatedRecord[row[0]][2]
                    row[3] = formatedRecord[row[0]][3]
                    row[4] = formatedRecord[row[0]][4]
                    row[5] = formatedRecord[row[0]][
                        5]  # To test comment out this line so records can be deleted with ease.
                    row[6] = formatedRecord[row[0]][6]
                    updateCursor.updateRow(row)

        except arcpy.ExecuteError:
            print(arcpy.GetMessages(2))

    return editedRouteOid


def WriteLogFile(routeOids, cpOids, nanOids, nonMonotonicOids):
    # Set progressor label.
    arcpy.SetProgressorLabel("Writing to output log file...")

    if routeOids or cpOids or nanOids or nonMonotonicOids:
        # Create output file in scratch folder.
        outputDir = arcpy.env.scratchFolder
        outputLogName = 'UpdateCpOutput' + r".log"
        outputFileLog = os.path.join(outputDir, outputLogName)

        txtFile = open(outputFileLog, 'w')

        if routeOids:
            # Sort oids for legibility.
            sortedOids = sorted(routeOids)

            # Write to log file.
            txtFile.write('Route OID(s) that had calibration points added:\n')
            txtFile.write('%s' % ','.join(map(str, sortedOids)))
            txtFile.write('\n\n\n')

        if cpOids:
            # Sort oids for legibility.
            sortedOids = sorted(cpOids)

            # Write to log file.
            txtFile.write('Calibration Point OID(s) that had Z values changed:\n')
            txtFile.write('%s' % ','.join(map(str, sortedOids)))
            txtFile.write('\n\n\n')

        if nanOids:
            # Sort oids for legibility.
            sortedOids = sorted(nanOids)

            # Write to log file.
            txtFile.write('Route OID(s) that are uncalibrated: \n')
            txtFile.write('%s' % ','.join(map(str, sortedOids)))
            txtFile.write('\n\n\n')

        if nonMonotonicOids:
            # Sort oids for legibility.
            sortedOids = sorted(nonMonotonicOids)

            # Write to log file.
            txtFile.write('Route OID(s) that have non-monotonic measures: \n')
            txtFile.write('%s' % ','.join(map(str, sortedOids)))

        txtFile.close()

        # Add message of log file location.
        arcpy.AddMessage('Log file at %s' % outputFileLog)


##******************************************##
##       Fix Z Values Functions             ##
##******************************************##


def GetAdjustZValuesForCalibrationPoints(networkPath, networkFields, calibrationPointPath, calibrationFields,
                                         tolerances, nanOids):
    # Set progressor bar and label.
    featureCount = int(arcpy.GetCount_management(networkPath)[0])
    arcpy.SetProgressor("step", "Checking Z values on calibration points...",
                        0, featureCount, 1)

    # Go route by route and check calibration points.
    alteredCps = {}
    routes = {}
    routesToIgnore = []
    for row in arcpy.da.SearchCursor(networkPath,
                                     ["OID@", networkFields.RouteId, networkFields.FromDate, networkFields.ToDate,
                                      "SHAPE@"]):

        # Store row in tuple for readability.
        routeValues = RouteInfo(Oid=row[0], RouteId=row[1], FromDate=row[2], ToDate=row[3], FromM=0, ToM=0, Network=0,
                                Geometry=row[4])

        # Ignore geometry that is null or not calibrated.
        if routeValues.Geometry is None or routeValues.Geometry.firstPoint.M is None or math.isnan(routeValues.Geometry.firstPoint.M):
            arcpy.SetProgressorPosition()
            continue

        # Skip routes with nan values
        if routeValues.Oid in nanOids:
            routesToIgnore.append(routeValues.RouteId)
            continue

        # Skip calibration points with nan values.
        if routeValues.FromM is None or math.isnan(routeValues.FromM):
            continue

        if routeValues.ToM is None or math.isnan(routeValues.ToM):
            continue

        # Get and store 1000 route ids at a time.
        if routeValues.RouteId not in routes:
            routes[routeValues.RouteId] = []

        routes[routeValues.RouteId].append(routeValues)

        # Check cps for this batch of routes.
        if len(routes) > 1000:
            GetPointsAtZ(calibrationPointPath, calibrationFields, routes, alteredCps, tolerances, routesToIgnore)
            routes.clear()
            for x in range(0, 1000):
                arcpy.SetProgressorPosition()


    # Check cps for any remaining routes.
    if len(routes) > 0:
        GetPointsAtZ(calibrationPointPath, calibrationFields, routes, alteredCps, tolerances, routesToIgnore)
        for x in range(0, len(routes)):
            arcpy.SetProgressorPosition()

    return alteredCps


def GetPointsAtZ(calibrationPointPath, calibrationFields, routes, alteredCps, tolerances, routesToIgnore):
    # Get route ids for routes and set where clause.
    routeIds = routes.keys()
    valueList = ["'%s'" % value for value in routeIds]
    whereclause = '%s IN (%s)' % (calibrationFields.RouteId, ','.join(map(str, valueList)))

    # Search Calibration point feature class. Store in dictionary based off routeId.
    calibrationDict = {}
    duplicateCpDict = {}
    for row in arcpy.da.SearchCursor(calibrationPointPath,
                                     ["OID@", calibrationFields.RouteId, calibrationFields.FromDate,
                                      calibrationFields.ToDate, calibrationFields.Measure, "SHAPE@"],
                                     where_clause=whereclause):

        # Store row in tuple for readability.
        cpValue = RouteInfo(Oid=row[0], RouteId=row[1], FromDate=row[2], ToDate=row[3], FromM=row[4], ToM=row[4],
                            Network=0, Geometry=row[5])

        # Skip routes with nan values
        if cpValue.RouteId in routesToIgnore:
            continue

        # Skip calibration points with nan values.
        if cpValue.FromM is None or math.isnan(cpValue.FromM):
            continue

        if cpValue.ToM is None or math.isnan(cpValue.ToM):
            continue

        if cpValue.RouteId not in calibrationDict:
            calibrationDict[cpValue.RouteId] = []

        calibrationDict[cpValue.RouteId].append(cpValue)

        # Look for duplicate cps at location.
        for cp in calibrationDict[cpValue.RouteId]:
            if (cp.Oid != cpValue.Oid and
                    Intersects(cp.FromDate, cpValue.FromDate, cp.ToDate, cpValue.ToDate) and
                    math.isclose(cp.Geometry[0].X, cpValue.Geometry[0].X, abs_tol=tolerances.XYTolerance) and
                    math.isclose(cp.Geometry[0].Y, cpValue.Geometry[0].Y, abs_tol=tolerances.XYTolerance)):

                if cpValue.RouteId not in duplicateCpDict:
                    duplicateCpDict[cpValue.RouteId] = []

                    duplicateCpDict[cpValue.RouteId].append(cp)
                    duplicateCpDict[cpValue.RouteId].append(cpValue)

    # Ignore cps with no measures.
    for routeId in calibrationDict:

        cpValues = calibrationDict[routeId]
        for cpValue in cpValues:

            # Skip points with duplicate cps at location.
            duplicateLocation = False
            if cpValue.RouteId in duplicateCpDict:
                for cp in duplicateCpDict[cpValue.RouteId]:
                    if cp.Oid == cpValue.Oid:
                        duplicateLocation = True
                        break

            if duplicateLocation:
                continue

            # Make sure there is valid geometry.
            if (cpValue.Geometry is not None and
                    cpValue.FromM is not None):

                if cpValue.RouteId not in routes:
                    continue

                # Check each time slice of the route.
                for route in routes[cpValue.RouteId]:

                    if Intersects(route.FromDate, cpValue.FromDate, route.ToDate, cpValue.ToDate):

                        # Get the point that is on the route. This will return the XY location with the
                        # correct Z. We will verify that XY did not change below.
                        newPoint = route.Geometry.queryPointAndDistance(cpValue.Geometry, False)[0]

                        # Make sure the Calibration Point is within the time span of the route, XY values
                        # match, and Z values do NOT match.
                        if (math.isclose(newPoint[0].X, cpValue.Geometry[0].X, abs_tol=tolerances.XYTolerance) and
                                math.isclose(newPoint[0].Y, cpValue.Geometry[0].Y, abs_tol=tolerances.XYTolerance) and
                                not math.isclose(newPoint[0].Z, cpValue.Geometry[0].Z, abs_tol=tolerances.ZTolerance)):

                            # Store in a dict with the altered oid and new geometry. We cannot update here as it would
                            # result in an edit log record.
                            if cpValue.Oid not in alteredCps:
                                alteredCps[cpValue.Oid] = []

                            alteredCps[cpValue.Oid].append(newPoint)

    # Handle duplicate cps.
    for routeId in duplicateCpDict:

        # Get list of cps at this location.
        cpValues = duplicateCpDict[routeId]

        # Should never happen. Just for safety.
        if len(cpValues) < 2:
            continue

        # Cps are inserted as pairs. We will need to
        # iterate each set of pairs.
        pairs = len(cpValues) / 2

        i = 0
        while (i <= pairs):

            firstCp = cpValues[i]
            secondCp = cpValues[i + 1]

            i += 2

            # swap values based on measure.
            if firstCp.FromM > secondCp.FromM:
                firstCp, secondCp = secondCp, firstCp

            # Find route that these points intersect.
            for route in routes[routeId]:

                if Intersects(route.FromDate, firstCp.FromDate, route.ToDate, firstCp.ToDate):

                    firstGeometry = None
                    secondGeometry = None
                    firstPointFound = False;

                    # Find the first and second occurrences of this point.
                    for part in route.Geometry:
                        for pnt in part:
                            if (math.isclose(pnt.X, firstCp.Geometry[0].X, abs_tol=tolerances.XYTolerance) and
                                    math.isclose(pnt.Y, firstCp.Geometry[0].Y, abs_tol=tolerances.XYTolerance)):

                                if not firstPointFound:
                                    firstPointFound = True
                                    firstGeometry = pnt
                                else:
                                    secondGeometry = pnt

                    # Update geometry of first occurrences.
                    if firstGeometry is not None:

                        sr = route.Geometry.spatialReference
                        newPoint1 = arcpy.PointGeometry(firstGeometry, sr, True, True)

                        if firstCp.Oid not in alteredCps:
                            alteredCps[firstCp.Oid] = []

                        alteredCps[firstCp.Oid].append(newPoint1)

                    # Update geometry of second occurrences.
                    if secondGeometry is not None:

                        sr = route.Geometry.spatialReference
                        newPoint2 = arcpy.PointGeometry(secondGeometry, sr, True, True)

                        if secondCp.Oid not in alteredCps:
                            alteredCps[secondCp.Oid] = []

                        alteredCps[secondCp.Oid].append(newPoint2)

    return


def UpdateCalibrationRecords(calibrationPointPath, editedCps, workspace):
    # Set progressor label.
    arcpy.SetProgressorLabel("Updating calibration points...")

    # Create where clause to get edited cps.
    oids = editedCps.keys()
    valueList = ["%s" % value for value in oids]
    whereclause = '%s IN (%s)' % ("OBJECTID", ','.join(map(str, valueList)))

    # To prevent edit log records, we will need to do this in two steps:
    # 1. Null out the existing geometry for the point record.
    # 2. Update the geometry field with the new geometry.
    # These are done with two separate cursors to avoid confusion.
    try:

        # Step 1: Null out the existing geometry for the point record.
        with arcpy.da.UpdateCursor(calibrationPointPath, ['OID@', 'SHAPE@'], whereclause) as updateCursorRemoveGeometry:

            for row in updateCursorRemoveGeometry:
                row[1] = None
                updateCursorRemoveGeometry.updateRow(row)

        # Step 2: Update the geometry field with the new geometry.
        with arcpy.da.UpdateCursor(calibrationPointPath, ['OID@', 'SHAPE@'], whereclause) as updateCursorAddGeometry:

            for row in updateCursorAddGeometry:
                row[1] = editedCps[row[0]][0]
                updateCursorAddGeometry.updateRow(row)

    except arcpy.ExecuteError:
        print(arcpy.GetMessages(2))

    # return the updated oids.
    return oids


##******************************************##
##            Utility Functions             ##
##******************************************##

def GetPoint(routeInfo, measure, tolerances):
    firstVertex = routeInfo.Geometry.firstPoint

    # iterate geometry to find vertex before/after measure.
    for part in routeInfo.Geometry:
        for vertex in part:

            # Found vertex with the measure. Return it.
            if math.isclose(vertex.M, measure, rel_tol=tolerances.MTolerance, abs_tol=tolerances.MTolerance):
                return vertex

            # Vertex measure is less than measure.
            if vertex.M < measure:
                firstVertex = vertex

            # Vertex measure is greater than measure. Use distances
            # to find the point at the measure we want.
            if vertex.M > measure and firstVertex.M - vertex.M > 0:
                # Get distances.
                firstVertexDistance = routeInfo.Geometry.queryPointAndDistance(firstVertex, False)[1]
                secondVertexDistance = routeInfo.Geometry.queryPointAndDistance(vertex, False)[1]

                # Interpolate distance.
                distanceBetween = secondVertexDistance - firstVertexDistance
                pointDistance = (((firstVertex.M - measure) * distanceBetween) / (
                            firstVertex.M - vertex.M)) + firstVertexDistance

                # Return point at distance.
                return routeInfo.Geometry.segmentAlongLine(firstVertexDistance, pointDistance, False).lastPoint

    # Return default (shouldn't ever get here),
    return firstVertex;


def CompareDate(date1, date2, lesser, greater):
    # Change null dates to min/max dates
    if date1 == None:
        date1 = datetime.datetime.max
    if date2 == None:
        date2 = datetime.datetime.max

    if lesser:
        return date1 < date2
    elif greater:
        return date1 > date2
    else:
        return date1 == date2


def Intersects(fromDate1, fromDate2, toDate1, toDate2):
    # Change null dates to min/max dates
    if fromDate1 == None:
        fromDate1 = datetime.datetime.min
    if toDate1 == None:
        toDate1 = datetime.datetime.max
    if fromDate2 == None:
        fromDate2 = datetime.datetime.min
    if toDate2 == None:
        toDate2 = datetime.datetime.max

    # Returns if dates intersect.
    return ((fromDate1 <= fromDate2 and (toDate2 <= toDate1 or toDate1 == datetime.datetime.max)) or
            (fromDate2 <= fromDate1 and (toDate1 <= toDate2 or toDate2 == datetime.datetime.max)))


def GetFeatureDataset(FC):
    # Returns the path to the Feature Dataset.
    fcPath = arcpy.Describe(FC).catalogPath
    fcHome = os.path.dirname(fcPath)

    if arcpy.Describe(fcHome).dataType == "FeatureDataset":
        return fcHome
    else:
        return None


def IsFeatureLayer(feature, parameter):
    # Returns if the input feature is a layer.
    input = arcpy.Describe(feature).catalogPath
    if 'https:' in input:
        parameter.setErrorMessage("Feature layers or layers from a service are not supported")
        return True

    return False


def GetLrsMetadata(feature, validate=False, parameter=None):
    # Find if feature is in LRS dataset
    lrsDatasetPath = GetFeatureDataset(feature)
    if not lrsDatasetPath:
        if validate:
            parameter.setErrorMessage(
                "The data is not in an LRS feature dataset. Move your data to a feature dataset and then run the Modify LRS tool and try again.")
            return None
        else:
            arcpy.AddError(
                "The data is not in an LRS feature dataset. Move your data to a feature dataset and then run the Modify LRS tool and try again.")

    # Find LRS metadata.
    datasetName = lrsDatasetPath.split('\\')[-1]
    try:
        desc = arcpy.Describe(lrsDatasetPath + '\\' + datasetName)
    except Exception as e:
        if validate:
            parameter.setErrorMessage(
                "The data is not in an LRS feature dataset. Move your data to a feature dataset and then run the Modify LRS tool and try again.")
            return None
        else:
            arcpy.AddError(
                "The data is not in an LRS feature dataset. Move your data to a feature dataset and then run the Modify LRS tool and try again.")

    lrsMetadata = desc.LrsMetadata
    if not lrsMetadata:
        if validate:
            parameter.setErrorMessage(
                "Cannot find LRS. Verify data is in LRS feature dataset and run Modify LRS tool and try again.")
            return None
        else:
            arcpy.AddError(
                "Cannot find LRS. Verify data is in LRS feature dataset and run Modify LRS tool and try again.")

    return lrsMetadata


def GetNetworkFromMetadata(lrsMetadata, inputFeature, validate=False, parameter=None):
    # if gdb, get just the network name..
    inputName = inputFeature.split('\\')[-1]

    # If sde, get just the network name.
    inputName = inputName.split('.')[-1]

    # Get LRS name from network
    root = ET.fromstring(lrsMetadata)

    # Get Networks in LRS.
    rootNetworks = root.findall('Networks')

    # Find input Network in LRS.
    for networks in rootNetworks:
        for network in networks:
            name = network.attrib['PersistedFeatureClassName']
            if name == inputName:
                return network

    if validate:
        parameter.setErrorMessage("The Network parameter is not a valid LRS Network feature.")
    else:
        arcpy.AddError("The Network parameter is not a valid LRS Network feature.")

    return None


def GetCalibrationPointFromMetadata(lrsMetadata, inputFeature, validate=False, parameter=None):
    # if gdb, get just the Calibration Point name..
    inputName = inputFeature.split('\\')[-1]

    # If sde, get just the Calibration Point name.
    inputName = inputName.split('.')[-1]

    root = ET.fromstring(lrsMetadata)
    calibrationPointLRSName = root.attrib['CalibrationPointFCName']
    if calibrationPointLRSName is not None:
        if calibrationPointLRSName != inputName:
            if validate:
                parameter.setErrorMessage("The Calibration Point parameter is not a valid LRS feature.")
                return None
            else:
                arcpy.AddError("The Calibration Point parameter is not a valid LRS feature.")
        else:
            return calibrationPointLRSName
    else:
        if validate:
            parameter.setErrorMessage("Error getting calibration point feature class from LRS metadata.")
        else:
            arcpy.AddError("Error getting calibration point feature class from LRS metadata.")

    return None


def GetNetworkFields(network):
    # Returns NamedTuple of Fields.
    routeId = network.attrib['PersistedFeatureClassRouteIdFieldName']
    fromDate = network.attrib['FromDateFieldName']
    toDate = network.attrib['ToDateFieldName']
    networkId = network.attrib['NetworkId']

    return Fields(routeId, fromDate, toDate, "", networkId)


def GetCalibrationPointFields(lrsMetadata):
    # Returns NamedTuple of Fields.
    root = ET.fromstring(lrsMetadata)
    rootFieldNames = root.findall('FieldNames')

    # Find input Network in LRS.
    for fields in rootFieldNames:
        for field in fields.iter('CalibrationPoint'):
            routeId = field.attrib['RouteId']
            fromDate = field.attrib['FromDate']
            toDate = field.attrib['ToDate']
            measure = field.attrib['Measure']
            networkid = field.attrib['NetworkId']

    return Fields(routeId, fromDate, toDate, measure, networkid)


def GetTolerances(feature):
    # Returns NamedTuple of Tolerances.
    SR = arcpy.Describe(feature).spatialReference
    return Tolerance(SR.XYTolerance * 2, SR.ZTolerance, SR.Mtolerance * 2)


def StartEditSession(workspace):
    # Start edit session.
    edit = arcpy.da.Editor(workspace)
    edit.startEditing(False, False)
    edit.startOperation()
    return edit


def StopEditSession(edit):
    # Stop edit session.
    edit.stopOperation()
    edit.stopEditing(True)
    return


##******************************************##
##              Named Tuples                ##
##******************************************##


class RouteInfo(NamedTuple):
    Oid: int
    RouteId: str
    FromDate: datetime.datetime
    ToDate: datetime.datetime
    FromM: float
    ToM: float
    Network: str
    Geometry: arcpy.Polyline


class Fields(NamedTuple):
    RouteId: str
    FromDate: str
    ToDate: str
    Measure: str
    NetworkId: str


class Tolerance(NamedTuple):
    XYTolerance: float
    ZTolerance: float
    MTolerance: float




