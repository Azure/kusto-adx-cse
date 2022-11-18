# Data Ingestion implementation guidance  

* [Data Ingestion building blocks](#data-ingestion-building-blocks)
    * [Ingestion types](#ingestion-type)
    * [Tables](#table)
    * [Data mappings](#data-mappings)
    * [Delivery Mechanism](#delivery-mechanism)
* [Common Ingestion Patters](#common-ingestion-patterns)
    * [Lambda vs Kappa](#kappa-vs-lamba)
    * [IoT routed telemetry](#iot-routed-telemetry)
    * [Flight Recorder](#flight-recorder-pattern)
    * [Partitionned landing zone approach](#partitionned-landing-zone-approach)
    * [Dead lettering](#dead-lettering)
* [Data staging in Data Explorer](#data-staging-in-data-explorer)
    * [Update policies usage and patterns](#update-policies-usage-and-patterns)
    * [Handling schema evolution](#handling-schema-evolution)
* [Ingestion optimization and considerations](#ingestion-optimization-and-considerations)
    * [File Type and compression](#file-type-and-compression)
    * [Compression](#compression)
    * [Cluster size](#cluster-sizes)
* [Ingestion Monitoring](#ingestion-monitoring)
    * [Ingestion Insights](#ingestion-insights)
    * [Ingestion failures](#ingestion-failures)


Ingesting data in data explorer has many components and approaches, this document will list the most common ones and help you decide which is the most appropriate.
## Data Ingestion building blocks
Ingestion is fundamentally composed of four concepts:
1.	Ingestion type
1.	A table to land the data
1.	Data mapping for that table
1.	And finally, a delivery mechanism for the data

### Ingestion type
There are two fundamental ingestion types. It is important to decide upfront which one to use:

#### Micro batch
Micro batching is when the cluster will pull from a data source based on a batching policy. When certain conditions are met, the cluster will seal a batch and pull from the delivery mechanism (See below)
#### Streaming
Streaming ingestion is available only with Messaging delivery mechanism (i.e. Event Hub and IoT Hub). Unlike micro-batching, the service does not wait for conditions to be met for ingestion to start. Instead, the cluster will dedicate resource for a process to run and continuously watches for new messages to arrive. Near real time ingestion is achieved notwithstanding network induced latency and other transient conditions upstream from the cluster.
### Table
Data explorer needs a table to land the data, that table must match the corresponding Data mapping definition (See below). 
### Data mappings
Data mappings are used to map some or all of the attributes/columns of the original data source to the destination table. If the data schema is well known and won’t change, you can map all attributes to corresponding columns in the destination table. 

You can also keep some variable parts of the data separate and map them to a dynamic field for downstream processing. See “Dealing with schema evolution” section for more details.

Mappings are tied to the file format they ingest. Both row-oriented (CSV, JSON, AVRO and W3CLOGFILE), and column-oriented (Parquet and ORC) mappings are available.
### Delivery mechanism
Ultimately, data must be delivered to Data Explorer through some kind of delivery mechanism. They are quite numerous and can be found in our ingestion documentation page. Here are a high level generalization of the most common delivery mechanisms: 
#### Messaging based ingestion
This approach uses either Event Hub or IoT hub as a data source. Both uses the service bus component as their base service and can be pulled from natively by ADX. This is by far the more robust ingestion path because of the inherent robustness and reliability of the pub sub approach. We also provide a Kafka connect package that can leverage Kafka topics as a data source.

Works best for:
-	Data already available in the messaging service
-	Live data with high volume and frequency
-	Semi structured data such as JSON and less frequently XML
-	Interoperability with other services that support Event Hub as a target endpoint (e.g. Azure Diagnostic logs)

Considerations:
-	If you have complex routing logic or data prep requirement pre-ingestion, we recommend you use a combination of upstream event hubs with Azure functions to act on the data before landing it in a “final hub” See common ingestion patterns below for more details.
-	Ensure that if other consumers are consuming the data in the event hub, they use their own Consumer groups to not interfere with the checkpointing that ADX will use.
-	Messaging services are the only azure services currently supported for Streaming ingestion.
#### Storage based ingestion
This approach involves landing files in a storage account and through the event grid service, a serverless ingestion chain is triggered and the file is queued for ingestion by the ADX engine. Note this assumes we’re using azure storage as a source but many other forms of batch ingestion into ADX are available. But ultimately, they are all ingested by the engine the same way so the guidance below sill applies.

Works best for:
-	Historical backfill or large one-time ingestion
-	The schema of the file is static or relatively stable 
-	The data producer is not available/capable of streaming in the data

Considerations:
-	Best to partition the files so that their individual size is roughly 1 Gb uncompressed. This is the ideal “per batch” size that the engine is aiming for.
-	If you need fine grain control of batch loading, additional transformation before loading and speed of ingestion is not a top priority, consider using Azure Data Factory to get a more traditional ETL control.
#### Orchestrators
Well known orchestrators can also be the delivery mechanisms. For example, Azure Data Factory supports Data Explorer both as a source and a sink. Just like any other ingestion, you will need to specify a table and mappings. Ingestion type is inherently batch for orchestrator. Other orchestrator examples are Azure Logic Apps and Power Automate. 
#### Programmatic (aka. SDKs)
You can also programmatically ingest data into data explorer using one of the many SDKs we support. Like any other ingestion, you will need to specify a table and mappings. Ingestion type can be both Streaming or Batching
## Common Ingestion patterns
Think of the following patterns as tools in your toolbox. We recommend you get familiar with all of them and then see which combination works for you.
### Kappa vs Lamba
There are two commonly accepted architecture for big data platforms, Lamba with both a batch and stream layer and Kappa with its streaming layer only.
ADX is skewed towards the Kappa end of the spectrum. It minimizes the batching times (essentially micro batching) and will favor speed and high throughput. 
### IoT routed telemetry
![Iot Routed Telemetry diagram](/bestpractices/ImplementationGuidance/images/RoutedTelemetry.png)
The IoT routed telemetry uses IoT hub as a landing area with subsequent built-in routing to direct the data to multiple event hubs. In this scenario, some data element in the IoT data messages allows IoT Hub to route the message to a specialized Event Hub. A classic example is IoT devices sending three types of messages: Telemetry, State and Alerts. Imagine a data element called “MessageType” which can take only one of three states described above. In this scenario, we can instruct IoT hub to route the Telemetry message to a dedicated Event Hub. Thus, we can have ADX ingest that data in a corresponding Telemetry table. This has the obvious advantage of frontloading the routing of data upstream of Data Explorer therefore saving computations. Note the “dead letter” hub which we’ll discuss further down in this section.
### Flight recorder pattern
![Flight Recorder Pattern Diagram](/bestpractices/ImplementationGuidance/images/Flight%20recorder.png)
This approach is a simplified and generalized version of the use case above. The messaging system such as Event Hub or Kafka is used as a “flight recorder” meaning that no data routing or processing happens in the service themselves and all downstream processing is delegated to the stream processor (Data explorer in this case). Note we also typically see a raw archive of the messages being captured for compliance reasons and forensics in case something fails in the stream processing. 
Note that the routing is performed using a feature called update policies which we will discuss further down in the data transformation section.
### Partitionned landing zone approach
In this pattern, we use a storage account as a landing zone where files will land to be ingested into ADX. This leverages Data Explorer’s event grid ingestion. Here is a high level workflow of how it works: 

![Event Grid Ingestion](/bestpractices/ImplementationGuidance/images/eventgrid%20ingestion.png) 

The storage account (aka: the landing zone) is partitioned using folders that represents either tables (ideal use case) or some logical entity. Imagine then a simple storage account with a root folder and two distinct subfolder root\Telemetry and root\Alerts. 

![Partitionned Landing Zone Diagram](/bestpractices/ImplementationGuidance/images/Partitionned%20landing%20zones.png)

We can then use ADX event grid ingestion and create two distinct data connections to the same storage account. The difference will be that each data connection will use a filter that will make it so the event trigger will only occur when the container name equals the filter parameters. See here for more details.  
### Dead lettering
This is not a pattern per se but something to consider when building the types of we have been discussing. Dead lettering, simply put, refers to the “else” case in an “if-then” type of scenario. Specifically, when you route data, whether it being upstream from ADX or inside ADX itself, always consider adding a “dead letter queue”. Let’s use the “message type” example from earlier. The system expects only one of three values {Telemetry, State and Alerts} but as we all know, data is seldom this compliant so it’s always safe to have a default route that will send message that have either a null value or an unexpected value to a specific event hub or table (conceptually referred to as the “dead letter queue”). That way you ensure that data is never lost, and unexpected or faulty data can be detected and remediated quickly.
## Data staging in Data Explorer
Data Explorer can be a very effective tool to stage data after it lands in the cluster. Note that this is entirely optional. It’s possible the data does not need any post-processing and can be immediately mapped to a table. This seldom happens however, and some form of staging will occur. This is done using update policies to create what’s colloquially known as the “land and expand” pattern (see section below).
### Update policies usage and patterns
#### The land and expand pattern
The main principle of this pattern is that you will land the data in a staging table that contains only one column of type string (if the data is tabular) or Dynamic (if the data is hierarchical like a JSON file). This is achieved at ingestion using a simple mapping file that maps the entirety of the data to that single column. This step achieves the “land” part of the pattern.
The next step is to “expand” the data using update policies. At a high level, an update policy is a KQL query that is executed on the data in a source table and the output of that query is saved in a destination table. Thus the “expand” logic is captured in the KQL code of the update policy. For example, one might have a large array in a JSON file from which we may want to extract a specific element and ignore the rest. This element extraction would be done in KQL in the update policy.
Note that this approach has limitations. The incoming message (or file) must be of reasonable size. The String and Dynamic data types are limited to an uncompressed size of 1Mb (220). For example, if you have one message that has a day’s worth of data in one large JSON array, that will fail since the size of the JSON will likely exceed 1Mb.
#### Routing
It’s common to perform routing inside the ADX engine as a variation of the land and expand pattern. 
Note: As mentioned earlier, it can be more cost effective to route data upstream from ADX. Make sure you analyse your use case and try out different scenarios.
Routing in ADX would first land the “raw” data in a landing table, as explained earlier, and would then execute as many update policies as there are potential “routes”. Let us reuse the example from earlier where an incoming JSON has a “MessageType” attribute that can take on one of three values { Telemetry, State and Alerts }. We would then use three update policies:

- ExtractAlerts: where we’d have a KQL script that would essentially use a | where MessageType == "alerts" clause and save the output to an Alert table
- ExtractState: where we’d have a KQL script that would essentially use a | where MessageType == "state" clause and save the output to an state table
- ExtractTelemetry: where we’d have a KQL script that would essentially use a | where MessageType == "Telemetry" clause and save the output to an Telemetry table
- ExtractDeadLetters: where we’d have a KQL script that would essentially use a | where MessageType !in ("alerts","Telemetry","state") clause and save the output to a dead letter table

### Handling schema evolution
The nature of high-speed data flow is that the schema of the data being sent is likely to change. Data Explorer is rather permissive when it comes to schema change, for example if it expects a field to be present but is missing, the engine will simply return a blank field. Similarly, a blank field is the result of a certain datatype mismatch (i.e. if you cast a string as an int for example). 

We should not ignore the fact that schema changes will happen, and we should be as intentional as possible about managing it. Implementing proper data hygiene and the careful assembly of well-known patterns will make this possible.

####  Property bags and dynamic fields
The simplest way to deal with schema change is to leverage a data explore data type called Dynamic to handle variability. Consider the following JSON message:
``` json
{
    "MessageID": 12345,
    "MessageType": "DeviceHealth",
    "DeviceInfo": [
        {
            "ModelName": "Model-T",
            "SerialNumber": "ABC123"
        }
    ],
    "DeviceHealth": [
        {
            "Timestamp": "2022-11-18 20:00:00.000000",
            "Temperature": 24.7,
            "Pressure": 101.5
        }
    ]
}
```
There are two arrays here with an unknown number of element in them. Consider the DeviceInfo array. Today it has two element (model name and SerialNumber). Maybe in the future we will add cellular connectivity and a Cell_Id attribute will be added. 

If we know with certainty that Model Name and Serial Number will always be present, we can go ahead and materialize them as columns in the table but what about the Cell_Id?

This is where a Dynamic field is helpful. You can instruct Data Explorer to save the content of the array to a dynamic field for future growth. That way you will have both existing columns and if additional fields to appear, you will have the opportunity to materialize them either dynamically when querying or by modifying your existing table to add a new column and subsequently modify your update policy to take the new field into account. 

Whichever route you take, there is no data loss.

#### Schema versioning + Routing + Dead Lettering
This is not a pattern per se but more a technical hygiene item that we should treat as a requirement. 

Whenever we send data to Data Explorer whether it be JSON or Tabular data, we should include a field (ideally as part of the first few static field) a Schema Version number or ID. Schema versioning will allow us to combine two other well known patterns to achieve our goal. 

We will first use a form of Routing in the sense that our update policies will include a where clause that will limit the intake of raw data to only those schemas we support. 

We will then use Dead lettering to handle the “unknown” schemas or a variation of the pattern to handle special version of the schema. For example, you may have a new “Beta” version of incoming messages and want to process those independently from the “production” messages. 

You can achieve that using a dedicated update policy to process only version 2a of a schema and send it to a separate table. When you are ready you can then gracefully cutover and deprecated older message processing logic.

Note that this assumes you are using the “land and expand” pattern as the entry point since we cannot apply a strong mapping to a variable schema. We must also entertain the notion of offloading that work to the messaging bus upstream from ADX when applicable. 

This combination of patterns also applies to an Event hub + Azure function service assembly as well.

## Ingestion optimization and considerations
The following are general guidance to optimize ingestion 
### File Sizes
When ingesting large files, be mindful that Data Explorer is a distributed platform. Therefore, ingesting individual very large files will not scale well. 

We recommend a file size around 1 Gigabyte uncompressed as the ideal ingestion files size. This way the engine will have a much more optimized path to distribute the ingestion across the cluster nodes.

Supported file types for storage based ingestion are listed in this doc page: Data formats supported by Azure Data Explorer for ingestion. | Microsoft Docs
Please refer to the summary table here for maximum file size dependent on ingestion method
### Compression
Also note the supported built-in compression. At this time only zip and gzip are supported.

Note: Other file types have implicit compression like Parquet. Please note that when ingesting these files, ADX must first decompress them before writing them on this. While performance enhancements have been made to the ingestion engine, when possible, use an uncompressed format like CSV as input for maximum ingestion throughput.
### Cluster sizes
Data explorer will strive to distribute and multi-thread the ingestion as best it can. Therefore, the cluster size will have a significant impact on ingestion throughput. A cluster with 12 nodes with high specs SKUs will perform significantly better than a basic sku 2 node clusters. Be mindful of the cluster capacity policy which governs resource allocations when trying to optimize ingestion speed.
## Ingestion monitoring
### Ingestion Insights
In the Insights blade on Azure portal in Azure Synapse Data Explorer’s sidebar menu, you can get granular insights into the ingestion process completion. All successful and failed ingestions by database and by table are reported here. In the case of batch ingestion, it also provides detailed component-level charts to monitor the batch ingestion process through various stages. Please note that you need to export Diagnostic Settings for the following categories (Succeeded ingestion, failed ingestion, Ingestion batching) to Log Analytics workspace to generate the insights as shown in the graphic below.
## Ingestion Failures
Data Explorer also provides a control command to get the list of all ingestion failures. Please refer to Ingestion failures - Azure Data Explorer | Microsoft Learn for more information. Please refer to Ingestion error codes in Azure Data Explorer | Microsoft Learn when using SDKs to ingest data. Please refer to Streaming ingestion failures - Azure Data Explorer | Microsoft Learn when using Streaming Ingestion
 


