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
-- Build table of species seasonal/regional use of ancillary data
smAnc AS (
	SELECT	i.strUC AS strUC
		  ,	a.strSpeciesModelCode AS strSpeciesModelCode
		  , CAST(ysnHandModel AS int) AS intHandModel
		  , CAST(ysnHydroFW AS int) AS intHydroFW
		  , CAST(ysnHydroOW AS int) AS intHydroOW
		  , CAST(ysnHydroWV AS int) AS intHydroWV
		  , CASE
				WHEN (strSalinity Is Null OR strSalinity = 'All Types') THEN 0
				ELSE 1
			END AS intSalinity
		  , CASE
				WHEN (strStreamVel Is Null OR strStreamVel = 'All Types') THEN 0
				ELSE 1
			END AS intStreamVel
		  , CASE
				WHEN strEdgeType Is Null THEN 0
				ELSE 1
			END AS intEdgeType
		  , CASE
				WHEN strUseForInt Is Null THEN 0
				ELSE 1
			END AS intUseForInt
		  , CAST(cbxContPatch AS int) AS intContPatch
		  ,	CAST(cbxNonCPatch AS int) AS intNonCPatch
		  , CASE
				WHEN intPercentCanopy Is Null THEN 0
				ELSE 1
			END AS intPercentCanopy 
		  , CASE
				WHEN intAuxBuff Is Null THEN 0
				ELSE 1
			END AS intAuxBuff
		  , CASE
				WHEN strAvoid Is Null THEN 0
				ELSE 1
			END AS intAvoid
		  ,	CAST(ysnUrbanExclude AS int) AS intUrbanExclude
		  ,	CAST(ysnUrbanInclude AS int) AS intUrbanInclude
		  , CASE
				WHEN (intElevMin Is Null OR intElevMin < 1) THEN 0
				ELSE 1
			END AS intElevMin
		  , CASE
				WHEN intElevMax Is Null THEN 0
				ELSE 1
			END AS intElevMax

	FROM GapVert_48_2001.dbo.tblModelAncillary a 
		 INNER JOIN GapVert_48_2001.dbo.tblModelInfo i
			ON a.strSpeciesModelCode = i.strSpeciesModelCode
	WHERE	i.ysnIncludeSubModel = 1 
	),

/*
	Identify species ancillary data use across seasonal/regional submodels.
	This sums up how many submodels have at least one ancillary parameter selection.
	Return only species whose submodel ancillary tally total is 0 meaning NO
	submodels use ANY ancillary parameter
*/
NonAncillary AS (
	SELECT
		strUC
	FROM smAnc
	GROUP BY strUC
	HAVING
		SUM ( intHandModel +
			  intHydroFW +
			  intHydroOW +
			  intHydroWV +
			  intSalinity +
			  intStreamVel +
			  intEdgeType +
			  intUseForInt +
			  intContPatch +
			  intNonCPatch +
			  intPercentCanopy +
			  intAuxBuff +
			  intAvoid +
			  intUrbanExclude +
			  intUrbanInclude +
			  intElevMin +
			  intElevMax ) = 0

	),

/*
	Pull out species whose models include at least one map unit selection
	that is a forested map unit.
	NOTE: This criterion is only for primary map units. Secondary map units are ignored
	Also, include only those species for whom there are valid submodels.
*/
ForestSelected AS 
(SELECT
		tblMapUnitDesc.intLSGapMapCode,
		tblMapUnitDesc.strLSGapName,
		tblMapUnitDesc.intForest,
		tblSppMapUnitPres.strSpeciesModelCode,
		tblSppMapUnitPres.ysnPres,
		tblModelInfo.ysnIncludeSubModel,
		tblModelInfo.strUC
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

/*
	Pull out the scientific and common names from the taxa table
*/
Taxa AS
(SELECT strUC, strSciName, strComName
FROM tblTaxa)

/*
	Combine the non-ancillary, at least one forested map unit selection, and
	corresponding taxa scientific name and common name sub-queries into final output

*/


SELECT 	Taxa.strSciName AS ScientificName,
		Taxa.strComName AS CommonName,
		Taxa.strUC AS SC,
		strSpeciesModelCode AS SMC,
		ForestSelected.intLSGapMapCode AS MUCode,
		ForestSelected.strLSGapName AS MUName,
		CASE 
			WHEN 
			  SUBSTRING(ForestSelected.strSpeciesModelCode, 8, 1)='y'
			  THEN 'year-round'
			WHEN
			  SUBSTRING(ForestSelected.strSpeciesModelCode, 8, 1)='s'
			  THEN 'summer'
			WHEN
			  SUBSTRING(ForestSelected.strSpeciesModelCode, 8, 1)='w'
			  THEN 'winter'
		END AS Season,
		
		CASE 
			WHEN 
			  SUBSTRING(ForestSelected.strSpeciesModelCode, 9, 1)='1'
			  THEN 'Northwest'
			WHEN
			  SUBSTRING(ForestSelected.strSpeciesModelCode, 9, 1)='2'
			  THEN 'Upper Midwest'
			WHEN
			  SUBSTRING(ForestSelected.strSpeciesModelCode, 9, 1)='3'
			  THEN 'Northeast'
			WHEN
			  SUBSTRING(ForestSelected.strSpeciesModelCode, 9, 1)='4'
			  THEN 'Southwest'
			WHEN
			  SUBSTRING(ForestSelected.strSpeciesModelCode, 9, 1)='5'
			  THEN 'Great Plains'
			WHEN
			  SUBSTRING(ForestSelected.strSpeciesModelCode, 9, 1)='6'
			  THEN 'Southeast'
		END AS Region

FROM
	ForestSelected INNER JOIN NonAncillary
	ON
	ForestSelected.strUC = NonAncillary.strUC
	INNER JOIN Taxa
	ON ForestSelected.strUC = Taxa.strUC"""

# Make a dataframe of the forest/non-ancillary species map unit associations
dfSppMUs = psql.read_sql(sqlWHR, conn)

# Merge the dataframes from the WHR and Analytc dbs using the columns
# that have map unit 4-digit codes: MUCode and level3 respectively
dfSppMUs_Macro = pd.merge(left=dfSppMUs, right=dfMacro, how='inner',
                      left_on='MUCode', right_on='level3')
dfSppMUs_Macro=dfSppMUs_Macro.sort_values(by=['SMC'])

# Export to CSV file
dfSppMUs_Macro.to_csv(workDir + "Species-Habitat-Macrogroups.csv")






