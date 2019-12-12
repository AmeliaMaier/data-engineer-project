# Data Engineer Project Software Design Documentation
The goal of this project is to get an idea of:
* Create a system to ingest movie data
* Create a database to store the ingested data
* Provide reports that give information based on the data

# Overview

The system will recieve monthly csv files that need to be injested and incorporated into the existing database and reports. The end product needs to be scalable and reusable for future similar projects. 

In order to be as modular as possible, the system will be made up of dynamic micro-services (currently represented by functions in the Main.py file). Each operation: ingestion, transformation, or reporting; will be a seperate service. This allows for changing the business logic in reporting without having to change or retest ingestion. The micro-services will be called by a workflow managing system that allows for scheduling, concurrent service calls and dependancy tracking. The reporting layer will be tables that are updated after each data ingestion so that the webservice call for information can be completed quickly and industry standard BI tools can be put on top of it in the future (Looker is not compatible with PSQL materialized views).

# Assumptions

* The database used is PSQL
    * The combination of Python, Pandas, and PSQL handles most basic datatype and special character issues, making the system easier to maintane. 
* The monthly data files are delta files.
    * This reduces the need for advanced optimization of the ingestion process.
* The vender fixes the json like format of the nested data in movie_metadata to follow json format guidelines.
    * This would allow the current workaround for unpacking the data to much faster and would remove the data conversion failures due to quotes being included in the data.
        * This is currently the bottleneck in ingesting the files.
* The csv heavy flow currently completed will only be used until a SQL drive flow is completed.
    * Taking data from csv, to csv, then from csv to SQL is inefficient and adds difficulties to the project.
* The countries and languages tables will be filled and maintaned from the international standards they are based on, not from the csvs recieved from the vendor.
* Most delete functionality will not be available.
    * Marking movies as deleted when information changes and adding a new row allows easy tracking of why reports changed.
    * Marking data linkages (like which languages where used in a movie) as deleted would require a seperate API unless the vendor confirms that all applicible information is sent anytime a datapoint changes.
    * Deleting something like a company doesn't make since. Even if they go out of business or are bought out, they made movies in the past and that information needs to be tracked.
* The usability of Pandas makes up for its slightly lower efficiency.
    * Most data engineers know pandas and are used to working with its extensive list of cleaning and transformation options. While it is slower at basic funtionality than a dictionary, it is far better auto-casting, integrating with database, and integrating with machine learning models like K Nearest Neighbors (for filling in missing data).


# Data Model

![Data Model](https://github.com/AmeliaMaier/data-engineer-project/blob/master/src/data_files/Movie%20Data%20Model.pdf "Data Model")

The data model is seperated into four primary sections: source, data warehouse, reporting, and support. The design has a focus on long term auditability.
* Source
    * This section is intended to be the begining of the pipeline where the csvs are read in with as few changes as possible. By providing a source repository: you can support full data tracking through the system, rebuild the data warehouse if the transformation/cleaning logic changes and needs to be applied to historic data, and provide full auditability for users.
* Data Warehouse
    * This section stores all the data in the cleanest version that is currently available. It is normalized to optimize for developer level work and is not intended for end users to be aware of.
* Reporting
    * This is a denormalized section intended to make reporting as easy and fast as possible. This section will contain most of the business logic that changes quickly: ie - how is profit calculated for a company if some of its films have budget but haven't been shown yet and some of its films work a partnership with other companies? This layer would normally be either views, materialized views, or tables. Since it will be exposed by an API, views would be highly inefficient as they have to be rebuilt ever time a new connection loads them. Materialized views are fast because they sit in active memory, but with large data sets they tend to exceed the available tempory file size for a normally sized system. Materialized views in PSQL also aren't compatible with all BI tools. Tables are relatively fast to pull data from, especially if they are indexed correctly, are compatible with all industry standard BI tools, and take the same amount of time to populate as materialized views. Therefore, this layer will be tables.
* Support
    * This is where support tables are kept, like the data mapping table. By holding the data mapping information in a table, a data dictionary is fairly easy to create and changes to the mapping only have to be done in one place.

# Maintainability

The primary concern with maintanablity is if bug fixes and refactoring can be done without breaking current functionality. In order to ensure this, a unit test suite would need to be added to the project and kept up to date with changes in functionality and business logic. This should be expected to take about as long as creating the project at the begining but will reduce the regression testing time substantially.

# Security

* Database
    * Standard database security measures should be taken:
        * Report connections should not have access to anything outside the report layer.
        * Connection(s) used by the data pipeline should not be used by anyone/anything else.
        * PPI information should be hashed where possible.
* API
    * If internal:
        * Without access to PPI
            * A basic username and password that is reset regularly will probably be fine
        * With access to PPI
            * A standard OAth package should be used
    * If external:
        * A standard OAth package should be used

