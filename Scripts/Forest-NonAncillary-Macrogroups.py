# -*- coding: utf-8 -*-
"""
Created on Fri Jun  7 12:06:00 2019

@author: mjrubino
"""




import pyodbc
import pandas as pd
import pandas.io.sql as psql

#%matplotlib notebook

pd.set_option('display.max_columns', 10)

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#            ++++ Directory Locations ++++
workDir = 'C:/Data/USGS Analyses/NVC-Analyses/Scripts/'


#############################################################################################
################################### LOCAL FUNCTIONS #########################################
#############################################################################################


## --------------Cursor and Database Connections--------------------

def ConnectToDB(connectionStr):
    '''
    (str) -> cursor, connection

    Provides a cursor within and a connection to the database

    Argument:
    connectionStr -- The SQL Server compatible connection string
        for connecting to a database
    '''
    try:
        con = pyodbc.connect(connectionStr)
    except:
        connectionStr = connectionStr.replace('11.0', '10.0')
        con = pyodbc.connect(connectionStr)

    return con.cursor(), con

## ----------------Database Connection----------------------

def DBConnection(dbname):
    '''
    Returns a cursor and connection within the GAP analytic database.
    '''
    # Database connection parameters
    dbstr = """DRIVER=SQL Server Native Client 11.0;
                    SERVER=CHUCK\SQL2014;
                    UID=;
                    PWD=;
                    TRUSTED_CONNECTION=Yes;
                    DATABASE={0};"""
    dbConStr = dbstr.format(dbname)

    return ConnectToDB(dbConStr)


#############################################################################################
#############################################################################################
#############################################################################################
    

# Connect to a local database
cur, conn = DBConnection('GAP_AnalyticDB')

# Make an SQL that pulls out NVC macrogroups from the Analytic db
sqlGA = """SELECT
            level3,
            nvc_macro AS Macrogroup
        FROM
            GAP_AnalyticDB.dbo.gap_landfire
		WHERE
			level3 > 0"""

# Make a dataframe of the macrogroups with the 4-digit code
dfMacro = psql.read_sql(sqlGA, conn)

# Delete the previous db connection variables and reset them for
# connecting to the WHR database
del cur, conn
cur, conn = DBConnection('GapVert_48_2001')

# Make an SQL that gets data for species who have at least one forested map unit
# and have NO ancillary data constraints
sqlWHR = """WITH
NonAncillary AS
(SELECT	strSpeciesModelCode,
		ysnHandModel,
		ysnHydroFW,
		ysnHydroOW,
		ysnHydroWV,
		ysnHydroSprings,
		strSalinity,
		strStreamVel,
		intFlowAccMin,
		intFlowAccMax,
		strEdgeType,
		intEdgeEcoWidth,
		strUseForInt,
		strForIntBuffer,
		cbxContPatch,
		cbxNonCPatch,
		intContPatchSize,
		intContPatchBuffIn,
		intContPatchBuffFrom,
		intNonCPatchPerc,
		intNonCPatchArea,
		intPercentCanopy,
		intAuxBuff,
		strAvoid,
		ysnUrbanExclude,
		ysnUrbanInclude,
		intElevMin,
		intElevMax,
		intSlopeMin,
		intSlopeMax
FROM tblModelAncillary
WHERE	strSpeciesModelCode Not Like '%0' AND
		ysnHandModel = 0 AND
		ysnHydroFW = 0 AND 
		ysnHydroOW = 0 AND 
		ysnHydroWV = 0 AND 
		ysnHydroSprings = 0 AND 
		strSalinity Is Null AND 
		strStreamVel Is Null AND 
		intFlowAccMin Is Null AND 
		intFlowAccMax Is Null AND 
		strEdgeType Is Null AND 
		intEdgeEcoWidth Is Null AND 
		strUseForInt Is Null AND 
		strForIntBuffer Is Null AND 
		cbxContPatch = 0 AND 
		cbxNonCPatch = 0 AND 
		intContPatchSize Is Null AND 
		intContPatchBuffIn Is Null AND 
		intContPatchBuffFrom Is Null AND 
		intNonCPatchPerc Is Null AND 
		intNonCPatchArea Is Null AND 
		intPercentCanopy Is Null AND 
		intAuxBuff Is Null AND 
		strAvoid Is Null AND 
		ysnUrbanExclude = 0 AND 
		ysnUrbanInclude = 0 AND 
		intElevMin Is Null AND 
		intElevMax Is Null AND 
		intSlopeMin Is Null AND 
		intSlopeMax Is Null
),



ForestSelected AS 
(SELECT
		tblMapUnitDesc.intLSGapMapCode,
		tblMapUnitDesc.strLSGapName,
		tblMapUnitDesc.intForest,
		tblSppMapUnitPres.strSpeciesModelCode,
		tblSppMapUnitPres.ysnPres,
		tblModelInfo.ysnIncludeSubModel
FROM 
		tblMapUnitDesc FULL JOIN tblSppMapUnitPres 
		ON tblMapUnitDesc.intLSGapMapCode = tblSppMapUnitPres.intLSGapMapCode
		INNER JOIN tblModelInfo 
		ON tblSppMapUnitPres.strSpeciesModelCode = tblModelInfo.strSpeciesModelCode
WHERE 
		tblMapUnitDesc.intForest = 1 AND 
		tblSppMapUnitPres.ysnPres = 1 AND 
		tblSppMapUnitPres.strSpeciesModelCode Not Like '%m_' AND
		tblModelInfo.ysnIncludeSubModel = 1),


Taxa AS
(SELECT strUC, strSciName, strComName
FROM tblTaxa)

SELECT 	Taxa.strSciName AS ScientificName,
		Taxa.strComName AS CommonName,
		Taxa.strUC AS SC,
		NonAncillary.strSpeciesModelCode AS SMC,
		ForestSelected.intLSGapMapCode AS MUCode,
		ForestSelected.strLSGapName AS MUName,
		CASE 
			WHEN 
			  SUBSTRING(NonAncillary.strSpeciesModelCode, 8, 1)='y'
			  THEN 'year-round'
			WHEN
			  SUBSTRING(NonAncillary.strSpeciesModelCode, 8, 1)='s'
			  THEN 'summer'
			WHEN
			  SUBSTRING(NonAncillary.strSpeciesModelCode, 8, 1)='w'
			  THEN 'winter'
		END AS Season,
		
		CASE 
			WHEN 
			  SUBSTRING(NonAncillary.strSpeciesModelCode, 9, 1)='1'
			  THEN 'Northwest'
			WHEN
			  SUBSTRING(NonAncillary.strSpeciesModelCode, 9, 1)='2'
			  THEN 'Upper Midwest'
			WHEN
			  SUBSTRING(NonAncillary.strSpeciesModelCode, 9, 1)='3'
			  THEN 'Northeast'
			WHEN
			  SUBSTRING(NonAncillary.strSpeciesModelCode, 9, 1)='4'
			  THEN 'Southwest'
			WHEN
			  SUBSTRING(NonAncillary.strSpeciesModelCode, 9, 1)='5'
			  THEN 'Great Plains'
			WHEN
			  SUBSTRING(NonAncillary.strSpeciesModelCode, 9, 1)='6'
			  THEN 'Southeast'
		END AS Region

FROM NonAncillary INNER JOIN ForestSelected ON NonAncillary.strSpeciesModelCode = ForestSelected.strSpeciesModelCode
				  INNER JOIN Taxa ON SUBSTRING(NonAncillary.strSpeciesModelCode, 1, 6) = Taxa.strUC"""
# Make a dataframe of the forest/non-ancillary species map unit associations
dfSppMUs = psql.read_sql(sqlWHR, conn)

# Merge the dataframes from the WHR and Analytc dbs using the columns
# that have map unit 4-digit codes: MUCode and level3 respectively
dfSppMUs_Macro = pd.merge(left=dfSppMUs, right=dfMacro, how='inner',
                      left_on='MUCode', right_on='level3')
dfSppMUs_Macro=dfSppMUs_Macro.sort_values(by=['SMC'])

# Export to CSV file
dfSppMUs_Macro.to_csv(workDir + "Species-Habitat-Macrogroups.csv")






