#!/usr/bin/env python

import csv

filename = "free-zipcode-database"
datasetList = []
visitedList = []
tempList = []
finalList = []
columnsInTransposedDataSet = 0
try:
    with open(filename + "-numeric.csv", "rb") as infile, open(filename + "-transpose.csv", "wb") as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        headers = next(reader, None)
        print(headers)
        numberOfColumns = len(headers)
        columnsInTransposedDataSet = numberOfColumns
        print(numberOfColumns)
        restDataset = [row for row in reader]
        numberOfRows = len(restDataset)
        print(numberOfRows)
        for row in restDataset:
            for item in row:
                datasetList.append(item)
        actualListLength = len(datasetList) - 1
        headerIndex = 0
        for i in range(0, len(datasetList)):
            dataItem = datasetList[i]
            if dataItem not in visitedList:
                if headerIndex > numberOfColumns - 1:
                    headerIndex = 0
                tempList.append(headers[headerIndex])
                headerIndex = headerIndex + 1
                for j in range(0, columnsInTransposedDataSet):
                    columnValue = i+(j*numberOfColumns)
                    if columnValue <= actualListLength:
                        visitedList.append(datasetList[columnValue])
                        tempList.append(datasetList[columnValue])
                if len(tempList) == columnsInTransposedDataSet + 1:
                    writer.writerow(tempList)
            tempList = []
        print("done")
except Exception as ex:
    template = "An exception of type {0} occurred. Arguments:\n{1!r}"
    message = template.format(type(ex).__name__, ex.args)
    print(message)