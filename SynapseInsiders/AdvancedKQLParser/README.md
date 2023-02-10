# Building an advanced KQL parser

This lab will walk you through how to build an advanced parser for a complex variable schema file that is based on real life data.

## Learning objectives
**In this lab you will:**:
- :mortar_board: Learn how to chain multiple update policies together to parse variable schemas into different tables with final defined schemas
- :mortar_board: Learn how to use advanced KQL and how to use the DYNAMIC datatype to good effect in dealing with variable schemas
- :mortar_board: Learn how to build advanced user defined functions to make your code easy to read and maintain
- :mortar_board: Hopefully have some fun in the process

## Context

This challenge will involve messages sent by a GPS unit using the NMEA (National Marine Electronics Association) standard. This standard is fairly common for low power IoT devices, commercial and professional GPS units such as those you would find onboard a boat or ground penetrating radar. 

The standard is relatively difficult to understand in the sense that it is not self describing when it comes to its schema. Each message carries a different schema and represents a specific event. One message might indicate a GPS fix with longitude and latitude data and another might indicate which satellites were "pinged" at a specific moment in time. 

Understanding NMEA entirely is very much out of scope for this training so we will limit ourselves to two specific message types GGA and RMC (see below for an NMEA primer) that are interesting for this challenge.

If you want to know more please consult this [gitlab page](https://gpsd.gitlab.io/gpsd/NMEA.html#_nmea_standard_sentences) for an in depth look at the NMEA format. 

### Understanding the basic anatomy of an NMEA message

Each NMEA message can have a variable lenght and variable number of fields but will always have the same basic structure (see below):

| Character / relative position  | Description |
| ------------- | ------------- |
| First Character  | This will always be a dollar sign ($), indicating this is an NMEA message  |
| everything after the $ and the * is considered the "Message payload |
| Char 2 and 3  | This is refered to as the "TalkerID" Essentially a unique 2 letter string that categorizes the device that emitted that specific data point.  |
| Char 4, 5 and 6 | This is the "Sentence type" and is definitely the most important string since we will use it to derive the schema of the message |
| Char 7 up until the * character | This is the variable schema part of the message. It consists of a comma delimited string that will vary depending on the sentence type (see above) |
| * | Checksum delimiter. This indicate that following two character are the checksum for the message payload |
| last two character | The last two character of a NMEA always follow immediatly after the asterisk(*) and represent an 8 bit XOR checksum of the string bytes represented using a 2 character hexadecimal |

**Example**

Consider the following example:
**$GNGGA,001043.00,4404.14036,N,12118.85961,W,1,12,0.98,1113.0,M,-21.3,M*47**

| Character / relative position  | Description |
| ------------- | ------------- |
| First Character  | The expected "$" is present |
| Everything between the $ and the * | This is considered the "Message payload" and is what is validated against the checksum |
| Char 2 and 3  | Here "GN" can be decoded as meaning "Combination of multiple satellite systems (NMEA 1083)"  |
| Char 4, 5 and 6 | here "GGA" can be decoded as "Global Positioning System Fix Data"  |
| Char 7 up until the * character | these are the individual fields. We will decode real GGA messages in this challenge so stay tuned |
| * | Checksum delimiter |
| last two character | here if you [run an XOR checksum](https://www.scadacore.com/tools/programming-calculators/online-checksum-calculator/) check of the payload (i.e. What is between the **$** and the **\***) or (**GNGGA,001043.00,4404.14036,N,12118.85961,W,1,12,0.98,1113.0,M,-21.3,M**) you will get "**47**" as the hex value.  |

## The architecture
Below is the high level overview of what you will build:
![High level architecture](/SynapseInsiders/AdvancedKQLParser/Images/NMEA%20Parser.png)

## The build

For this challenge, we will ask you to:

1. Ingest the RAW messages using the [sample file here](/SynapseInsiders/SampleData/NmeaSample.csv) using One-click ingestion.
1. Land the data in a landing table called "**nmeaLanding**"
1. Create a "**f_PreParseNmea()**" function that will apply the variable schema and classify the messages. This function will be used to feed an update policy.
1. The update policy will feed a destination table called "**NmeaPreParse**"
1. Create a "**f_ParseGGA()**" and "**f_parse_RMC()**" functions that will extract the individual fields of the two message types we have identified. Each function gets assigned to an update policy
1. We then save the output with full schema applied in the corresponding tables named "**nmeaGGA**" and "**nmeaRMC**"
1. :mortar_board: **Challenge:** We will ask you to create another path for the sentence type **GSA**. The starting point will be to create a new function called **f_ParseGSA()** that will follow the same pattern as the other two you will have seen in this lab.
    1. **Important** You will also have to modify the **f_PreParseNmea()** function to handle the new message type
    
1. :mortar_board: **Challenge** We will ask you to land the formated GSA messages in a **nmeaGSA** table.
    1. **Hint** The schema for GSA messages can be found [here](https://gpsd.gitlab.io/gpsd/NMEA.html#_gsa_gps_dop_and_active_satellites)
1. :mortar_board: **Bonus Challenge:** Craft a "Dead letter" update policy that handles messages that are NOT of type GGA, GSA or RMC and store the results in a dead letter table.


## Creating the landing table and ingesting the sample data

1. Make sure you are logged in your cluster and contextualized on your database
1. Run the following code to create the table and assign a folder to is to keep things organized

``` Kusto
.execute database script <|
    .create table ['nmeaLanding']  (['data']:string) 
    .alter table nmeaLanding  folder "nmea.tables" 
```

### Use one-click ingestion to load the data and create the table

Note we are ingesting now only for you to see and explore the RAW data. We will clear the data and re-ingest later so you see how parsing is achieved.

1. Access your data explorer web interface at [https://dataexplorer.azure.com](https://dataexplorer.azure.com) 
1. Click on data management(in the right hand side blade) and then click "ingest data" ![Ingestion step 0](/SynapseInsiders/AdvancedKQLParser/Images/nmeaIngestion0.png)
1. Make sure to select "use existing table" option and select "nmeaLanding" in the drop down. ![Ingestion step 1](/SynapseInsiders/AdvancedKQLParser/Images/nmeaIngestion1.png)
1. Select "File for the source type and select the sample file that can be found [here](/SynapseInsiders/AdvancedKQLParser/Sample%20Data/output.txt) "![Ingestion Step 2](/SynapseInsiders/AdvancedKQLParser/Images/nmeaIngestion2.png)
1. Finaly, make sure the data format is set to "**TXT**" and hit "**Start Ingestion**"![Ingestion Step 3](/SynapseInsiders/AdvancedKQLParser/Images/nmeaIngestion3.png)

## Creating the pre-parsing function and destination table
This is the large function that will pre parse the messages. The code snippet below is heavyly commented and you should be able to follow along.

``` Kusto
// ===========================================================================================================
// ========================= Section 2: PreParse function object creation ====================================
// ===========================================================================================================
.create-or-alter function with 
    (
         folder = "nmea.parsing"
        ,docstring = "This function preParses the messages and provide the required routing data for downstream processing"
        ,skipvalidation = "false"
        //
    ) f_PreParseNmea() 
{
// ===========================================================================================================
// ========================= Kusto Query language NMEA message parser ========================================
// ===========================================================================================================
// == Date Created: 2023-01-22                                                                              ==
// == Author:       Gilles L'Hérault                                                                        ==
// == References:   https://gpsd.gitlab.io/gpsd/NMEA.html#_nmea_standard_sentences                          ==
// ===========================================================================================================
// == This script declares a few variables to start and a few user defined function design to make the      ==
// == code more readable. UDFs are prefixed with a "f_" for easy of identification when reading the code    ==
// == The first few "let" statement create "Dynamic" datatypes that will hold an object or more precisly    ==
// == a JSON property bag that represent the data structures of each known NMEA message. We will use        == 
// == this later when we split the messages using the comma delimiter and we will map each element to       ==
// == their respective schema column. This information has been obtained from various publicly available    ==
// == web sites referenced above.                                                                           ==
// ===========================================================================================================
//
// ===========================================================================================================
// == Then we create the "Struct", the data structures to help us shape the different NMEA sentences        ==
// ===========================================================================================================
let ggaStruct = dynamic
                    (
                        [
                            'TalkerID','FixTime','Latitude','LatitudeHemisphere','Longitude'
                           ,'LongitudePosition','QualityIndicator','SatellitesInUse','HorizontalDilution'
                           ,'AntennaAltitude','AntennaAltitudeUnit','GeoidalSeparation','GeoidalSeparationUnit'
                           ,'AgeOfDifferentialData','DifferentialReferenceStationID', 'Checksum'
                        ]
                    );
let rmcStruct = dynamic
                    (
                        [
                             'TalkerID','FixTime','Status','Latitude','LatitudeHemisphere','Longitude'
                            ,'LongitudePosition','SpeedKnots','TrackAngle','Date','MagneticVariationDegrees'
                            ,'MagneticVariationDirection','Checksum'
                        ]
                    );
// ===========================================================================================================================
// == Section 2.1 - Begin user defined functions section =====================================================================
// ===========================================================================================================================
// == PreFligh in-line function  =============================================================================================
// == This function will parse the message at a coarse level, i.e. extracting the fields that are always there and that we 
// == need for routing and actual field parsing.
// ===========================================================================================================================
let f_PreFlightParse = (NmeaLine:string) 
    {   // We wrapup our field definition with a check to see if it starts with $ to make sure the string is indeed an nmea 
        // sentence and we extract the talker id first (first 2 character after the $
        let MyTalkerId = iif(NmeaLine startswith "$",substring(NmeaLine,1,2),'Not an nmea sentence');
        // Then we get the sentence type 3 char after talker id
        let SentenceType = iif(NmeaLine startswith "$",substring(NmeaLine,3,3),'???');
        // We also grab the entire payload for later parsing. The payload is everything between the $ and the *
        let MessagePayload = iif(NmeaLine startswith "$",split((split(NmeaLine,'$').[1]),'*').[0],'');
        // we grab the checksum (two char after the * so we can write a function later on to check if the string is valid 
        // (future improvement to this parser)
        let Checksum = tostring(split(NmeaLine,"*").[1]);
        // We bag up the data as a set of key value pair. Bag_pack is used here so we can return a scalar dynamic field with 
        // our data. Since ADX does not let us use tabular expression inline with another tabular expression, we have to do 
        // this that way.
        bag_pack(
                 "TalkerId"         , MyTalkerId
                ,"SentenceType"     , SentenceType
                ,"MessagePayload"   , MessagePayload
                ,"Checksum"         , Checksum
                )
    }
;
// == End PreFLight in-line Function =========================================================================================
//
// ===========================================================================================================================
// == Section 2.2 - Parse GGA sentence Function ==============================================================================
// ===========================================================================================================================
// == This function will split the message payload and make an array of the fields. Then it will grabs the ggaStruct which 
// == contains the headers for a GGA message. Then we use bag_pack function to perform a "zip" operation where we pair up 
// == the headers and the extracted field
// == We will then have the ability to access the individual field using jsonpath in another query
// ===========================================================================================================================
let f_Process_gga = (MessagePayload:string) 
    {
        let MySplit = split(MessagePayload,',');
        let MyStruct = ggaStruct;
        bag_pack(
                 tostring(MyStruct.[0]) ,substring(tostring(MySplit.[0]),0,2)
                ,tostring(MyStruct.[1]) ,tostring(MySplit.[1]) 
                ,tostring(MyStruct.[2]) ,tostring(MySplit.[2]) 
                ,tostring(MyStruct.[3]) ,tostring(MySplit.[3])
                ,tostring(MyStruct.[4]) ,tostring(MySplit.[4])
                ,tostring(MyStruct.[5]) ,tostring(MySplit.[5])
                ,tostring(MyStruct.[6]) ,tostring(MySplit.[6])
                ,tostring(MyStruct.[7]) ,tostring(MySplit.[7])
                ,tostring(MyStruct.[8]) ,tostring(MySplit.[8])
                ,tostring(MyStruct.[9]) ,tostring(MySplit.[9])
                ,tostring(MyStruct.[10]),tostring(MySplit.[10])
                ,tostring(MyStruct.[11]),tostring(MySplit.[11])
                ,tostring(MyStruct.[12]),tostring(MySplit.[12])
                ,tostring(MyStruct.[13]),tostring(MySplit.[13])
                ,tostring(MyStruct.[14]),tostring(MySplit.[14])
            )
    };
// == End Parse GGA sentence Function ========================================================================================
// 
// ===========================================================================================================================
// == Section 2.3 Parse RMC sentence Function (see 2.2 for description =======================================================
// ===========================================================================================================================
let f_Process_rmc = (MessagePayload:string) 
    {
        let MySplit = split(MessagePayload,',');
        let MyStruct = rmcStruct;
        bag_pack(
                 tostring(MyStruct.[0]) ,substring(tostring(MySplit.[0]),0,2)
                ,tostring(MyStruct.[1]) ,tostring(MySplit.[1]) 
                ,tostring(MyStruct.[2]) ,tostring(MySplit.[2]) 
                ,tostring(MyStruct.[3]) ,tostring(MySplit.[3])
                ,tostring(MyStruct.[4]) ,tostring(MySplit.[4])
                ,tostring(MyStruct.[5]) ,tostring(MySplit.[5])
                ,tostring(MyStruct.[6]) ,tostring(MySplit.[6])
                ,tostring(MyStruct.[7]) ,tostring(MySplit.[7])
                ,tostring(MyStruct.[8]) ,tostring(MySplit.[8])
                ,tostring(MyStruct.[9]) ,tostring(MySplit.[9])
                ,tostring(MyStruct.[10]),tostring(MySplit.[10])
                ,tostring(MyStruct.[11]),tostring(MySplit.[11])
                ,tostring(MyStruct.[12]),tostring(MySplit.[12])
                ,tostring(MyStruct.[13]),tostring(MySplit.[13])
                ,tostring(MyStruct.[14]),tostring(MySplit.[14])
            )
    }    
;
// == End Parse RMC sentence Function ========================================================================================
// 
// == Section 2.4 preparse logic =============================================================================================
// see inline comment for explanation below
// ===========================================================================================================================
nmeaLanding
| extend  PreParseMsg    = f_PreFlightParse(data) // calling our function. This returns a dynamic field with our preparsed data
| extend  TalkerId       = tostring(PreParseMsg.TalkerId) // we extract the fields we need using jsonPath
         ,SentenceType   = tostring(PreParseMsg.SentenceType)
         ,MessagePayload = tostring(PreParseMsg.MessagePayload)
         ,Checksum       = tostring(PreParseMsg.Checksum)
| extend ParsedMessage   = case( // using this case expression, we invoke the correct function dependent on the sentencetype
                                 SentenceType == 'GGA' , f_Process_gga(PreParseMsg.MessagePayload)
                                ,SentenceType == 'RMC' , f_Process_rmc(PreParseMsg.MessagePayload)
                                // Add VTG and GSA type here
                                ,bag_pack("Error:","UnsupportedSentenceType","SentenceType",SentenceType) // some error handling
                                )                                                                         // Could be better...
} 
```
## Creating the NmeaPreParse table and associated update policy
The following script uses a neat KQL trick to use the function's output to create an empty table that's ready to receive the preParsed messages.

We then assign some meta data to the table for housekeeping and we then move on to create the update policy

``` Kusto
.execute database script <|
    // we use the set-or-append trick to create the empty destination table
    .set-or-append nmeaPreParse <| f_PreParseNmea() | limit 0 
    // we add a folder name to our table to make things tidy
    .alter table nmeaPreParse folder "nmea.tables" 
    // we create an update policy that will run our preparse function everytime something is written to the landing table
    .alter table nmeaPreParse policy update  
    @'[{"IsEnabled": true, "Source": "nmeaLanding", "Query": "f_PreParseNmea()", "IsTransactional": false, "PropagateIngestionProperties": true}]'
```

## Creating the helper functions
This code will create function that will prove useful for other function. In other words we are creating reusable code to simplify our life!

We will create three helper functions:

### Creating the f_FormatTimeString() function
The timestamps in NMEA messages are provided in hhmmss format. So to reconsruct a proper datetime, we must use the ingestion time and concatenate the YYYYMMDD portion with the HHMMSS portion from the message.

Note we are also adding a "folder" to organise this function according to what it does.
``` Kusto
.create-or-alter function with 
    (
         folder = "nmea.helper"
        ,docstring = "This function will reconstruct a true datetime using the partial time given in the nmea message"
        ,skipvalidation = "false"
        //
    ) f_FormatTimeString(MyTimeString:string, MyIngestionTime:datetime ) 
{
let FormatedNowDate       = tostring(format_datetime(MyIngestionTime, 'yyyy-MM-dd' ));
    let SplitTimeHours       = tostring(substring((split(MyTimeString,'.').[0]),0,2));
    let SplitTimeMinutes     = tostring(substring((split(MyTimeString,'.').[0]),2,2));
    let SplitTimeSeconds     = tostring(substring((split(MyTimeString,'.').[0]),4,2));
    let SplitTimeMiliseconds = tostring(split(MyTimeString,'.').[1]);
    todatetime(strcat_delim(' ',FormatedNowDate,(strcat_delim('.',(strcat_delim(':',SplitTimeHours,SplitTimeMinutes,SplitTimeSeconds)),SplitTimeMiliseconds))));
}
``` 
### Creating the f_processLatitude() function.
The latitude in NMEA messages is given in two parts one is in (D)DDMM.MMMM (i.e.: Degrees, Minutes). The second part is the hemisphere (North or South). We must translate that to regular lattitude that other systems can leverage.

``` Kusto
.create-or-alter function with (folder = "nmea.helper", docstring = "This function will convert the latitude data and use the hemisphere info to return the correct value", skipvalidation = "true") 
    f_processLatitude(MyLatitude:string, MyHemishere:string) 
        {
            let MyLatSplit = split(MyLatitude,"."); // splitting to get ddmm and mmmmm on the other
            let MyMinutes = toreal(strcat(substring(MyLatSplit.[0], -2,2),'.',MyLatSplit.[1])); //negative substring to get the mm part of 
            //the ddmm. We are doing this from right to left because degrees can be 2 or 3 digits long (1-360)
            let MyDegrees = toreal(iif(strlen(tostring(MyLatSplit.[0])) == 4,substring(MyLatSplit.[0],0,2), substring(MyLatSplit.[0],0,3)));
            let MyRealLatitude = toreal(MyDegrees + MyMinutes/60);
        toreal(iif(MyHemishere != 'N', MyRealLatitude*-1,MyRealLatitude))
        }
``` 
### Creating the f_processLongitude() function.
Same principle as above but for longitude, the hemispheres are either West or East

``` Kusto
.create-or-alter function with (folder = "nmea.helper", docstring = "This function will convert the longitude data and use the hemisphere info to return the correct value", skipvalidation = "true") 
    f_processLongitude(MyLongitude:string, MyHemishere:string) 
        {
            let MyLonSplit = split(MyLongitude,".");
            let MyMinutes = toreal(strcat(substring(MyLonSplit.[0], -2,2),'.',MyLonSplit.[1]));
            let MyDegrees = toreal(iif(strlen(tostring(MyLonSplit.[0])) == 4,substring(MyLonSplit.[0],0,2), substring(MyLonSplit.[0],0,3)));
            let MyRealLongitude = toreal(MyDegrees + MyMinutes/60);
        toreal(iif(MyHemishere != 'E', MyRealLongitude*-1,MyRealLongitude))
        } 
``` 

## Creating the GGA and RMC sentence parsing function and destination table

The following section will describe the message specific parsing functions:

### Creating the f_ParseGGA() function 
This is the function that will extract the relevant GGA specific field from the preparsed messages. Note here how we define in-line function that are specific to GGA messages. Their purpose is to decode certain field into human readable strings.

``` Kusto
.create-or-alter function with 
    (
         folder = "nmea.parsing"
        ,docstring = "This function fully parses the GGA messages according to the known schema from the specs"
        ,skipvalidation = "false"
        //
    ) f_parseGGA() 
{
// ==========================================================================================================
// == Section 5.1  Create GGA specific in-line function  ====================================================
// ==========================================================================================================
// Note the inline functions below can be thought of as reference tables to decodes the values
// Ideally they would be reference tables somewhere but we are looking to keep this parser as 
// portable as possible. In a production system where the number of items to decode may be large
// we recommend you build real materialized tables with these values and you lookup the decoded
// string using the lookup function.
let f_processGPSQualityIndicator= (MyGpsQualityIndicator:string) 
{
case(
        MyGpsQualityIndicator == '0', 'fix not available'
       ,MyGpsQualityIndicator == '1', 'GPS fix' 
       ,MyGpsQualityIndicator == '2', 'Differential GPS fix' 
       ,MyGpsQualityIndicator == '3', 'PPS fix' 
       ,MyGpsQualityIndicator == '4', 'Real Time Kinematic' 
       ,MyGpsQualityIndicator == '5', 'Float RTK' 
       ,MyGpsQualityIndicator == '6', 'estimated (dead reckoning)' 
       ,MyGpsQualityIndicator == '7', 'Manual input mode' 
       ,MyGpsQualityIndicator == '8', 'Simulation mode' 
       ,''
    )   
};
let f_processTalkerID = (MyTalkerId:string)
{
case( 
        MyTalkerId == 'GN', 'Combination of multiple satellite systems'
       ,MyTalkerId == 'GP', 'Global Positioning System receiver'
       ,'Unknown TalkerId'
     )  
};
let f_processSentenceType = (MySentenceType:string)
{
case( 
        MySentenceType == 'GGA', 'Global Positioning System Fix Data'
       ,MySentenceType == 'RMC', 'Recommended Minimum Navigation Information'
       ,'Unknown SentenceType'
     )  
};
nmeaPreParse
| where  SentenceType == 'GGA'
| extend IngestionTIme = ingestion_time()
| extend FixTime = tostring(ParsedMessage.FixTime)
        ,Latitude = ParsedMessage.Latitude
        ,LatitudeHemisphere=ParsedMessage.LatitudeHemisphere
        ,Longitude =ParsedMessage.Longitude
        ,LongitudePosition=ParsedMessage.LongitudePosition
        ,QualityIndicator=ParsedMessage.QualityIndicator
        ,toint(SatellitesInUse=ParsedMessage.SatellitesInUse)
        ,toreal(HorizontalDilution=ParsedMessage.HorizontalDilution)
        ,toreal(AntennaAltitude=ParsedMessage.AntennaAltitude)
        ,tostring(AntennaAltitudeUnit=ParsedMessage.AntennaAltitudeUnit)
        ,toreal(GeoidalSeparation =ParsedMessage.GeoidalSeparation)
        ,tostring(GeoidalSeparationUnit=ParsedMessage.GeoidalSeparationUnit)
        ,tostring(AgeOfDifferentialData=ParsedMessage.AgeOfDifferentialData)
        ,tostring(DifferentialReferenceStationID=ParsedMessage.DifferentialReferenceStationID)
| extend FixTime            = f_FormatTimeString(FixTime,IngestionTIme)
        ,Longitude          = f_processLongitude(Longitude,LongitudePosition)
        ,Latitude           = f_processLatitude(Latitude,LatitudeHemisphere)
        ,QualityIndicator   = f_processGPSQualityIndicator(QualityIndicator)
        ,TalkerId = f_processTalkerID(TalkerId)
        ,SentenceType = f_processSentenceType(SentenceType)
| project  
         IngestionTIme,TalkerId,SentenceType,FixTime,Latitude
        ,Longitude,QualityIndicator,SatellitesInUse,HorizontalDilution
        ,AntennaAltitude,AntennaAltitudeUnit,GeoidalSeparation,GeoidalSeparationUnit
        ,AgeOfDifferentialData,DifferentialReferenceStationID,Checksum
}
``` 
### Creating the nmeaGGA table and update policy

Just like for pre parsing earlier, we use the function we just created to create the destination table and we connect it to the nmeaPreParse table via an update policy

``` Kusto
.execute database script <|
// Create the table using the function as the template
.set-or-append nmeaGGA <| f_parseGGA() | limit 0
// create the update policy binding the source table to the destination table
.alter table nmeaGGA policy update  
@'[{"IsEnabled": true, "Source": "nmeaPreParse", "Query": "f_parseGGA()", "IsTransactional": false, "PropagateIngestionProperties": true}]'
// assign the table to a folder for aesthetics
.alter table nmeaGGA folder "nmea.tables"
``` 

### Creating the f_ParseRMC() function

Similar to the above, we do the same but for RMC messages.

``` Kusto
.create-or-alter function with 
    (
         folder = "nmea.parsing"
        ,docstring = "This function fully parses the RMC messages according to the known schema from the specs"
        ,skipvalidation = "false"
        //
    ) f_parseRMC() 
{
// ==========================================================================================================
// == Section 6.1  Create RMC specific in-line function  ====================================================
// ==========================================================================================================
let f_processTalkerID = (MyTalkerId:string)
{
case( 
        MyTalkerId == 'GN', 'Combination of multiple satellite systems'
       ,MyTalkerId == 'GP', 'Global Positioning System receiver'
       ,'Unknown TalkerId'
     )  
};
let f_processSentenceType = (MySentenceType:string)
{
case( 
        MySentenceType == 'GGA', 'Global Positioning System Fix Data'
       ,MySentenceType == 'RMC', 'Recommended Minimum Navigation Information'
       ,'Unknown SentenceType'
     )  
};
let f_FormatNmeaDate = (MyNmeaDate:string) {
let CurrentMillenia = bin((toint(datetime_part('year',datetime(now)))),1000); 
let myYY = toint(substring(MyNmeaDate,4,2));
let myMM = tostring(substring(MyNmeaDate,2,2));
let myDD = tostring(substring(MyNmeaDate,0,2));
let MyYear = tostring(CurrentMillenia+myYY);
let MyFormatedNmeaDate = strcat_delim('-',MyYear,myMM,myDD);
todatetime(MyFormatedNmeaDate)
};
nmeaPreParse
| where  SentenceType == 'RMC'
| extend IngestionTIme = ingestion_time()
| extend FixTime = tostring(ParsedMessage.FixTime)
        ,Status = tostring(ParsedMessage.Status)
        ,Latitude = ParsedMessage.Latitude
        ,LatitudeHemisphere=ParsedMessage.LatitudeHemisphere
        ,Longitude =ParsedMessage.Longitude
        ,LongitudePosition=ParsedMessage.LongitudePosition
        ,MagneticVariationDegrees = toreal(ParsedMessage.MagneticVariationDegrees)
        ,MagneticVariationDirection = tostring(ParsedMessage.MagneticVariationDirection)
        ,TrackAngle= toreal(ParsedMessage.TrackAngle)
        ,SpeedKnots = toreal(ParsedMessage.SpeedKnots)
        ,Date = tostring(ParsedMessage.Date)
| extend FixTime        = f_FormatTimeString(FixTime,IngestionTIme)
        ,Longitude      = f_processLongitude(Longitude,LongitudePosition)
        ,Latitude       = f_processLatitude(Latitude,LatitudeHemisphere)        
        ,TalkerId       = f_processTalkerID(TalkerId)
        ,SentenceType   = f_processSentenceType(SentenceType)    
        ,NmeaDate       = f_FormatNmeaDate(Date)
        ,Status         = iif(Status == 'A', 'Valid', 'Warning')
| project  
         IngestionTIme,TalkerId,SentenceType,FixTime,Status, Latitude
        ,Longitude,SpeedKnots,TrackAngle,NmeaDate, MagneticVariationDegrees
        ,MagneticVariationDirection, Checksum
}
``` 

### Creating the nmeaRMC table and update policy

Just like for pre parsing earlier, we use the function we just created to create the destination table and we connect it to the nmeaPreParse table via an update policy

``` Kusto
.execute database script <|
// create the target table for RMC messages using the set-or-append trick
.set-or-append nmeaRMC <| f_parseRMC() | limit 0
// assign the table to a folder for aesthetics
.alter table nmeaRMC folder "nmea.tables"
// create the update policy binding the source table to the destination table
.alter table nmeaRMC policy update  
@'[{"IsEnabled": true, "Source": "nmeaPreParse", "Query": "f_parseRMC()", "IsTransactional": false, "PropagateIngestionProperties": true}]'
``` 

