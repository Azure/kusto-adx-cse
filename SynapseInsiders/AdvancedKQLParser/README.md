# Building an advanced KQL parser

This lab will walk you through how to build an advanced parser for a complex variable schema file that is based on real life data.

:mortar_board: 
## Learning objectives
**In this lab you will:**:
- Learn how to chain multiple update policies together to parse variable schemas into different tables with final defined schemas
- Learn how to use advanced KQL and how to use the DYNAMIC datatype to good effect in dealing with variable schemas
- Learn how to build advanced user defined functions to make your code easy to read and maintain
- Hopefully have some fun in the process

## Context

This challenge will involve messages sent by a GPS unit using the NMEA standard. This standard is fairly common for low power IoT devices, commercial and professional GPS units such as those you would find onboard a boat or ground penetrating radar. 

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
| everything after the $ and the * is considered the "Message payload |
| Char 2 and 3  | Here "GN" can be decoded as meaning "Combination of multiple satellite systems (NMEA 1083)"  |
| Char 4, 5 and 6 | here "GGA" can be decoded as "Global Positioning System Fix Data"  |
| Char 7 up until the * character | these are the individual fields. We will decode real GGA messages in this challenge so stay tuned |
| * | Checksum delimiter |
| last two character | here if you [run an XOR checksum](https://www.scadacore.com/tools/programming-calculators/online-checksum-calculator/) check of the payload (i.e. What is between the **$** and the **\***) or (**GNGGA,001043.00,4404.14036,N,12118.85961,W,1,12,0.98,1113.0,M,-21.3,M**) you will get "**47**" as the hex value.  |

## The build

For this challenge, we will ask you to:

1. Ingest the RAW messages using the [sample file here](/SynapseInsiders/SampleData/NmeaSample.csv) in a landing table called "**nmeaRaw**"
1. Create a "PreFlight parsing" function that will apply the variable schema and classify the messages. This function will be used to feed an update policy that will then save the data to a table called "**PreParseNmea**"
1. Create a "ParseNmea" function that will extract the individual fields of the two message types we have identified and save them in the corresponding tables named "**nmeaGGA**" and "**nmeaRMC**"
1. This training will walk you through how to do the above for both GGA and RMC sentence type. Your challenge will be to modify the KQL code so that it can handle an additional message type. We ask that you add support for the VTG message type. You can find the specs for this message type [here:](https://gpsd.gitlab.io/gpsd/NMEA.html#_vtg_track_made_good_and_ground_speed). **The sample file you downloaded contains an additional line of this type. Until you add support for this messagetype, this data will go in the "Unsupported" error handling.**

## Creating the landing table and ingesting the sample data

1. Make sure you are logged in your cluster and contextualized on your database
1. Run the following code to create the table and assign a folder to is to keep things organized

``` Kusto
.execute database script <|
    .create table ['nmeaLanding']  (['data']:string) 
    .alter table nmeaLanding  folder "nmea.tables" 
```

### Use one-click ingestion to load the data and create the table

Make sure the table name and table mapping name are correct

## Creating the pre-parsing function and destination table

## Creating the GGA and RMC sentence parsing function and destination table

