Zabbix-DellEMC-PowerMax - Zabbix Monitoring Integration for DellEMC PowerMax
=======================

This template and supporting script have been developed to integrate the DellEMC PowerMax into the Open-Source Monitoring Tool Zabbix (http://www.zabbix.com)

## Description

The DellEMC PowerMax and Zabbix integration leverages the Unisphere REST API to collect performance and capacity diagnostic level statistics from a Unisphere for PowerMax server (either the onboard eMGMT or external) and presents the PowerMax as a Host in Zabbix, with individual Applications for each object type.   Data is provided to Zabbix with a granularity of 5 minutes, this allows us to leverage the default statistics gathered by the PowerMax and not introduce any additional workload to the Unisphere server.

The integration consists of two components, a Template that is imported into Zabbix and a python script that is run from the Zabbix server.   The script performs Discovery operations of array components via Zabbix LLD and also collects the performance statistics.   We leverage the ‘Zabbix Trapper’ type for our items, using a python implementation of the Zabbix Sender protocol, which allows statistics collection to either be run as a scheduled ‘external check’ from within Zabbix or as a scheduled cron job on the system.   The cron option is provided for especially large environments where the external check could run longer than the Zabbix timeout in some configurations.    One instance of the script is run per PowerMax system.

## Installation

**Prerequisites**

1.  Zabbix 5.x (It should be compatible with 4.x, however has not yet been tested)
2.  Unisphere 9.2.0.0
3.  Python 3.6
4.  Python Modules
  1. PyU4V – Python Module for interaction with Unisphere for PowerMax
    * https://pypi.org/project/PyU4V/
  2. Py-zabbix – Python Module for interaction with Zabbix (pure python implementation for cross compatibility)
    * https://pypi.org/project/py-zabbix/

Please be sure that the correct Py-zabbix module is installed, there are two with very similar names.

**Installation**

Discovery Configuration 
1.  Place the zabbix_powermax.py python script in your external scripts directory.
2.  Update the zabbix_powermax.py script with the IP address and Port for the zabbix trapper on your server or agent.
3.  Update the zabbix_powermax.py script log file location if you prefer a location besides the default, be sure this location is writable by the zabbix user.
2.  Configure a PyU4V.conf file for you Unisphere for PowerMax installation as documented in the PyU4V documentation.   Store this file in a location accessible to the zabbix user.
5.  Test the zabbix_powermax.py script as the zabbix user from the command line with the following command:  zabbix_powermax.py --discovery --configpath <path to PyU4V.conf file> --array <array serial>
6.  Import the attached template into your zabbix installation
7.  Create a new Host in Zabbix named: "PowerMax <array serial>"  CASE IS IMPORTANT
8.  Create two Host Level Macros
  *  {$ARRAYID} - Serial of Array
  *  {$U4VPATH} - Path to PyU4V.conf file
9.  Link the DellEMC PowerMax Template to the newly created Host
10.  Be Patient, it will take about 30-40 minutes for discovery to be completed, you can monitor the log file as the discovery takes place.   

Statistics Collection Configuration Option 1 (CRON) -- Preferred
1.  As the Zabbix user test statistics collection with the following command:  zabbix_powermax.py --configpath <path to PyU4V.conf file> --array <array serial>, you should see the collections run in the log and data should appear in Zabbix.
2.  Configure a cron job to run this command every 5 minutes.   Simple as that.

Statistics Collection Configuration Option 2 (Zabbix Managed)
1.  Configure an item in Zabbix that runs the collection script with the appropriate parameters every 5 minutes.

**Troubleshooting**

* Common Troubleshooting
  * Check the serial/arrayid, it should start with leading 0's and be 12 Characters long.   For example HK0197900255 would be represented as 000197900255
  * Review the log files, often changing the log level to logging.DEBUG will yield more information about connectivity and data collection issues.   

* Discovery Issues
  * If nothing is discovered, make sure the path to python is correct at the top of the script and the modules are accessible by the zabbix user.   The environment created by Zabbix when running external LLD scripts is quite minimal.
  * Validate that the script runs as the Zabbix user successfully, if it does not validate the PyU4V.conf file is working and credentials are correct

* Stats Collection Issues 
  * Statistics are only collected if they are less than 5 minutes old (this is known as recency) you can tweak this setting in the script, but it will not increase the granularity of the statistics, just whether the script will collect and send them.  You may have to run the script multiple times in testing before data shows up in Zabbix.   The logs will tell you if the recency hasn't been met.  It is recommended to NOT change this setting unless you are testing something specific.
  * Validate that Diagnostic statistics are enabled on the PowerMax.  It is enabled by default but may have been disabled

## Implememted Objects  
**Currently Supported Objects**
-	General Array Health Scores
-	Array Level Statistics (Capacity, overall IOs, Read/Write distribution, etc.)
-	Frontend and Backend Director and Port level performance statistics
-	SRP performance statistics
-	Attached Host and Initiator Statistics
-	Storage Group performance statistics
-	Port Group performance Statistics

-	RDF Director and Port level performance statistics
-	RDF performance statistics 

**KPIs yet to be implemented** (Please reach out if you are leveraging these features)
-	FICON statistics
-	vVOL Storage Container Statistics
-	IP Interface Statistics
-	ExternalDisk Statistics (For example ProtectPoint I/O rates)

Licensing
---------
This software is provided under the MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
Status API Training Shop Blog About Pricing


Support
-------
Please file bugs and issues at the Github issues page.