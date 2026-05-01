Filing Query API
SEC EDGAR filing index page converted to JSON
SEC EDGAR filing index page converted to JSON and
searchable with the Query API
The Query API allows searching and filtering all 18+ million filings and exhibits published on the SEC EDGAR database since 1993 to present, with new filings being added in 300 milliseconds after their publication on EDGAR. The API accepts simple and complex search expressions and returns the metadata of matching EDGAR filings in JSON format including form type, accession numbers, filing date and times, filer information such as CIK, ticker and entity name, exhibits and attachments, URLs to the original SEC sources, and many more.

EDGAR filings are searchable by numerous parameters, such as: CIK (Central Index Key), ticker symbol or name of the filer, form type, filing date, accession number, item IDs in case of Form 8-K or Form 1-U filings, exhibit types, series and class contract IDs, and more. Refer to the list of searchable parameters to see all available search options.

The filing search capabilities of the Query API can be tested free of charge in our sandbox available here. The sanbdox showcases a variety of filing search examples and allows testing the API with own queries in a simple no-code environment.

Filing Example Response in JSON Format
{
    // Microsoft's 10-Q filing for Q1 2024
    accessionNo: "0000950170-24-048288",
    formType: "10-Q",
    periodOfReport: "2024-03-31",
    filedAt: "2024-04-25T16:06:24-04:00",
    ticker: "MSFT",
    cik: "789019",
    companyName: "MICROSOFT CORP (Filer)",
    linkToFilingDetails: "sec.gov/Archive ... 4048288/msft-20240331.htm",
    entities: [ ... ], // entities associated with filing: CIK, EIN, state of incorp., fiscal year, and more
    documentFormatFiles: [ ... ], // files and exhibits attached to filing: URL, type, size
    dataFiles: [ ... ], // XBRL data files
    // ... more fields
}
The filings database inlcudes all EDGAR form types without exception, from annual and quarterly reports (Form 10-K, 10-Q, 20-F, 40-F), event-driven filings such as Form 8-K and 6-K, insider trading activities (Form 3, 4, 5), over registration statements (Form S-1, S-3, S-8) and prospectuses (Form 424B2, 424B3, 424B4), to institutional investment manager and company filings (Form 13F, N-PORT) and more than 150 other types ever published. The database includes all filings ever published by any of the 800,000 EDGAR filer entities. A full list of all EDGAR form types is available on the form types page.

Dataset size:
All EDGAR filings and exhibits published since 1993 to present, including all form types and all filers.
Data update frequency:
New filings are indexed and searchable in less than 300 milliseconds after they are published on EDGAR.
Survivorship bias free:
Yes. The Query API and filings database include all EDGAR filings and exhibits filed since 1993 to present, from filer entities that are still active and those that are no longer active.
Common Use Cases
The EDGAR filing search capability of the Query API is often used as a first step in a multi-process filing ingestion pipeline. For example, the Query API is often used to find the most recently disclosed filings of a specific form type, such as 10-Qs or 8-Ks, to obtain the URLs of those filings, and then download the full text of the filings using the Filing Download API or to convert the filings to PDFs using the PDF Generator API.

Common use cases for the Query API include:

Submitting recently filed filing exhibits to regulatory authorities
Monitoring filer compliance with exchange rules
Powering internal insight analytics platforms
Backtesting quantitative investment strategies
Showing EDGAR filings on investor relationships websites
API Endpoint
In order to search and retrieve metadata of EDGAR filings, HTTP POST requests with the search expression as payload are sent to the following API endpoint:

https://api.sec-api.io
Supported HTTP methods: POST

Request and response content type: JSON

Authentication
To authenticate your API requests, use the API key shown in your user profile. You can utilize your API key in one of two ways. Choose the method that best fits your implementation:

Authorization Header: Include your API key as an Authorization header in your POST requests. For instance, before sending a POST request to https://api.sec-api.io, ensure the header is set as follows: Authorization: YOUR_API_KEY
Query Parameter: Alternatively, append your API key directly to the URL as a query parameter. For example, when making POST requests, use the URL https://api.sec-api.io?token=YOUR_API_KEY instead of the base API endpoint.
Request Parameters
To search, filter and retrieve metadata of EDGAR filings, send a HTTP POST request with a JSON-formatted payload to the API endpoint https://api.sec-api.io. The payload includes the search criteria, pagination and optional response sorting parameters, for example:

Example of JSON Request Payload to Find the 50 Most Recent 10-Q Filings
{
    "query": "formType:\"10-Q\"",
    "from": "0",
    "size": "50",
    "sort": [{ "filedAt": { "order": "desc" }}]
}
The API request payload includes the following fields, with the query field being required and the others optional:

query (string): Specifies the search criteria written in the Lucene syntax following the field:value format to search filings that include value in the specified field. For example, formType:"10-K" to find Form 10-K filings or ticker:AAPL to locate filings published by Apple. The query string allows searching the filings database by any available field in the response object as described in the Response Format section below. For example, filing lookups can be performed by the filer's CIK, series, class or contract IDs, date ranges across filing publication times and periods of reports, event item codes in Form 8-Ks or 1-Us, exhibit types, and more. Multiple search criteria can be combined using logical operators (AND, OR, NOT). For example, formType:"10-K" AND ticker:AAPL to find Form 10-K filings published by Apple. The maximum length of the query string cannot exceed 3500 characters. Examples of various filing search expressions are available in the Query String Examples section below and in the sandbox. A complete guide on how to use the Lucene syntax is available here.
from (string, optional) - Use the from property to specify the starting position of your search results, facilitating pagination. For instance, set from to 50 to skip the first 50 filings. The default is 0, and the maximum allowed value is 10,000, which is also the cap for the maximum number of filings returned per query string. To retrieve all filings in your search universe, increment from by the value of the size parameter (e.g., 50) until no more filings are returned or the 10,000 filing limit is reached. For example, use 0, 50, 100, and so on. If your query locates more than 10,000 filings, consider narrowing your search by refining your filter criteria, for example, using a date range filter to iterate over months or years. One such approach would be to search for filings with a filedAt date range filter, e.g. filedAt:[2021-01-01 TO 2021-01-31] (all filings from January 2021), then paginate through the results by incrementing from, and once completed, repeat the process for the next month, and so on.
size (string, optional) - Determines the number of filings returned per request. For instance, set size to 10 to retrieve 10 filings per request. The default value is 50, and the maximum allowed value is 50, which is the cap for the maximum number of filings returned per request.
sort (array, optional) - An array of objects that specify how the returned filings are sorted. The default sorting order is descending by filedAt datetime field, most recent filing first. The sorting order can be changed to ascending by setting order to asc. The default sorting order can be overridden by specifying a different sorting field. For example, to sort filings by periodOfReport in ascending order, set sort to [{ "periodOfReport": { "order": "asc" } }].
Default: [{ "filedAt": { "order": "desc" } }]
Query String Examples
The following list demonstrates different query examples that can be used to search EDGAR filings by various criteria.

Description
Example
Search EDGAR filings by filer entity properties
Search by CIK of filer
cik:12345
Search by multiple CIKs
cik:(123, 456, 789)
Search by ticker
ticker:AAPL
Search by tickers
ticker:(AAPL, MSFT, NVDA, TSLA)
Search by filer entity name
companyName:"Nissan Auto Receivables" AND companyName:Trust
Search by filer entity SIC
entities.sic:3714
Search by filer entity state
entities.stateOfIncorporation:DE
Search by filer entity IRS number
entities.irsNo:710388071
Search by file number
entities.fileNo:"001-36743"
Search EDGAR filings by filing properties
Search by form type
formType:"10-K"
Multiple form types
formType:"10-K" OR formType:"10-Q"
Excluding form types from search
formType:"10-K" AND NOT formType:"10-K/A"
Search by accession number of filing
accessionNo:"0001193125-23-265616"
Date range without time
filedAt:[2022-01-01 TO 2022-12-31]
Date range with time
filedAt:[2021-09-15T14:00:00 TO 2021-09-15T19:00:00]
Event item ID in Form 8-Ks
items:"2.02" AND formType:"8-K"
Multiple item IDs in Form 8-Ks
items:("2.02", "5.02") AND formType:"8-K"
By series ID
seriesAndClassesContractsInformation.series:S000011051
By class/contract ID
seriesAndClassesContractsInformation. classesContracts.classContract:C000120702
By ticker of class/contract
seriesAndClassesContractsInformation.classesContracts.ticker:ABRZX
By class/contract ID and form type
formType:"NPORT-P" AND seriesAndClassesContractsInformation. classesContracts.classContract:C000120702
By exhibit type
documentFormatFiles.type:*21* AND formType:"10-K"
By registration form of EFFECT types
formType:EFFECT AND registrationForm:"S-3"
Other examples
Match all filings
id:*
Find 10-Ks of companies by ticker
formType:"10-K" AND ticker:(AAPL, MSFT, NVDA, TSLA)
Find 10-Ks with images
formType:"10-K" AND documentFormatFiles.type:GRAPHIC
Good to Know
Search by CIK: cik:CIK_OF_INTEREST or cik:(CIK_1, CIK_2, ...) to find filings disclosed by a specific filer using its CIK, or filings from by multiple filers using their respective CIKs. When searching filings by the CIK of the filer, remove leading zeros from the CIK. For example, transform 0000320193 to 320193. An example CIK search can be performed in the sandbox here.
Search by ticker: ticker:TICKER_OF_INTEREST or ticker:(TICKER_A, TICKER_B, ...) to retrieve filings by one or more ticker symbols of the filing companies. Ticker symbols in the filing search are case-insensitive, meaning that ticker:AAPL and ticker:aapl will return the same results. When performing historical filing searches going back far in time, it is recommended to use a filer's CIK instead of the ticker symbol. This approach accounts for entities that underwent ticker changes or entities that have multiple security classes listed on exchanges trading under different tickers, such as Alphabet's Class A (ticker: GOOGL) and Class C (ticker: GOOG) shares. The Mapping API can be used to resolve and map ticker symbols to CIKs, and vice versa. An example ticker search can be performed in the sandbox here.
Search by company name: companyName:"NAME OF COMPANY" to perform a search by the company name. Enclose the company name in double quotes (e.g., companyName:"My Trust A") to treat the company name as a single search term. This is useful when searching for company names that include whitespaces or special characters like "-", "." and "&". If the company name is a single word, the double quotes are optional. If the company name includes whitespaces and double quotes are omitted, each word is treated as a separate search term combined with an implicit OR operator. For example, companyName:My Trust A is equivalent to companyName:My OR *:Trust OR *:A, which searches for filings with the words "My" in the companyName field, or "Trust" in any field, or "A" in any field.
Search by EDGAR form type: formType:"FORM TYPE OF INTEREST" or formType:("FORM TYPE A", "FORM TYPE B", ...) to search for filings by one or multiple EDGAR form types. Ensure to enclose form types in double quotes (e.g., formType:"10-K"). The Query API provides access to the entire EDGAR filings corpus, allowing searches across all form types without exception. A complete list of form types can be found here. An example form type search can be performed in the sandbox.
Search across dates: filedAt:[START_DATE TO END_DATE] to search for filings filed within a specific date and time range. Use square brackets [ ] to include the start and end dates in the search. The date format is YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS for specific times. The time component is optional. For example, filedAt:[2021-01-01 TO 2021-12-31] captures all filings from midnight January 1, 2021 to midnight December 31, inclusive. An example with a time component is filedAt:[2021-01-06T14:00:00 TO 2021-01-06T15:00:00], which returns all filings disclosed on January 6, 2021 between 2:00 PM and 3:00 PM (Eastern Time), inclusive. The filedAt value in a filing metadata object is reported in Eastern Time in reference to the timezone of the EDGAR system. For example 2021-01-12T16:00:54-05:00 is 4:00:54 PM Eastern Time on January 12, 2021. When performing date-time queries, there is no need to include the timezone offset ("-05:00" or "-04:00"). The API automatically converts all times to Eastern Time. An example of a date-time query is available in the sandbox here.
Nested Field Search: documentFormatFiles.type:"EX-21" AND formType:"10-K" - Use nested field search and combine conditions to filter filings where a specific document type is included in a particular form. For example, this query finds 10-K filings that include Exhibit 21 ("EX-21"). Nested fields can be found in documentFormatFiles, dataFiles, entities, holdings, and seriesAndClassesContractsInformation. Examples of nested field searches are available in our sandbox here (10-K with Exhibit 21), here (10-K with XBRL data) and here (class contract ID).
Request Examples
The following examples demonstrate a range of Query API requests. These queries are designed to help you retrieve specific EDGAR filings based on criteria such as form types, tickers of the filing companies, date ranges, and exhibit types. More examples can be found in the sandbox.

Find the most recent 10-Q filings
Return the 50 most recently filed 10-Q filings, sorted by their publication time filedAt, starting with the newest filings.

JSON
{
    "query": "formType:\"10-Q\"",
    "from": "0",
    "size": "50",
    "sort": [{ "filedAt": { "order": "desc" }}]
}
Response
Table
JSON
Ticker	Company	CIK	Form Type	Filed At		Period Of
Report	Filing	Filing
Details
Page	Complete
Submission
File	Entities
(CIK, IRS No, State, Fiscal Year End, File No, Film No, SIC)	Attachments	XBRL Files	...
NRIM	NORTHRIM BANCORP INC	1163370	10-Q	5/1/2026, 1:06:53 PM		3/30/2026	HTML	HTML	TXT	
1163370	920175752	AK	000-33501	26931396	6035	1231
EX-31.1, EX-31.2, EX-32.1, EX-32.2	EX-101.SCH, EX-101.CAL, EX-101.DEF, EX-101.LAB, EX-101.PRE, XML	...
ATMU	Atmus Filtration Technologies ...	1921963	10-Q	5/1/2026, 1:04:28 PM		3/30/2026	HTML	HTML	TXT	
1921963	061601605		001-41710	26931373	3714	1231
EX-19, EX-31.1, EX-31.2, EX-32.1, GRAPHIC, GRAPHIC, ...	EX-101.SCH, EX-101.CAL, EX-101.DEF, EX-101.LAB, EX-101.PRE, XML	...
PRLB	Proto Labs Inc	1443669	10-Q	5/1/2026, 12:58:14 PM		3/30/2026	HTML	HTML	TXT	
1443669	000000000	MN	001-35435	26931283	3440	1231
EX-31.1, EX-31.2, EX-32.1	EX-101.SCH, EX-101.CAL, EX-101.DEF, EX-101.LAB, EX-101.PRE, XML	...
ORI	OLD REPUBLIC INTERNATIONAL COR...	74260	10-Q	5/1/2026, 12:46:45 PM		3/30/2026	HTML	HTML	TXT	
74260	362678171	DE	001-10607	26931209	6351	1231
EX-31.1, EX-31.2, EX-32.1, EX-32.2	EX-101.SCH, EX-101.CAL, EX-101.DEF, EX-101.LAB, EX-101.PRE, XML	...
PUBLIX SUPER MARKETS INC	81061	10-Q	5/1/2026, 12:31:47 PM		3/27/2026	HTML	HTML	TXT	
81061	590324412	FL	000-00981	26931116	5411	1226
EX-31.1, EX-31.2, EX-31.3, EX-32.1, EX-32.2, EX-32.3, ...	EX-101.SCH, EX-101.CAL, EX-101.DEF, EX-101.LAB, EX-101.PRE, XML	...
ACA	Arcosa, Inc.	1739445	10-Q	5/1/2026, 12:11:09 PM		3/30/2026	HTML	HTML	TXT	
1739445	825339416	DE	001-38494	26930932	3440	1231
EX-2.1, EX-4.1, EX-4.2, EX-10.1, EX-31.1, EX-31.2, ...	EX-101.SCH, EX-101.CAL, EX-101.DEF, EX-101.LAB, EX-101.PRE, XML	...
CHD	CHURCH & DWIGHT CO INC /DE/	313927	10-Q	5/1/2026, 12:10:26 PM		3/30/2026	HTML	HTML	TXT	
313927	134996950	DE	001-10585	26930926	2840	1231
EX-10.1, EX-10.2, EX-31.1, EX-31.2, EX-32.1, EX-32.2, ...	EX-101.SCH, XML	...
MHK	MOHAWK INDUSTRIES INC	851968	10-Q	5/1/2026, 12:08:01 PM		4/3/2026	HTML	HTML	TXT	
851968	521604305	DE	001-13697	26930883	2273	1231
EX-31.1, EX-31.2, EX-32.1, EX-32.2, EX-95.1, GRAPHIC ...	EX-101.SCH, EX-101.CAL, EX-101.DEF, EX-101.LAB, EX-101.PRE, XML	...
AIG	AMERICAN INTERNATIONAL GROUP, ...	5272	10-Q	5/1/2026, 11:44:18 AM		3/30/2026	HTML	HTML	TXT	
5272	132592361	DE	001-08787	26930639	6331	1231
EX-10.1, EX-10.2, EX-10.3, EX-31, EX-32, GRAPHIC, ...	EX-101.SCH, EX-101.CAL, EX-101.DEF, EX-101.LAB, EX-101.PRE, XML	...
RHP	Ryman Hospitality Properties, ...	1040829	10-Q	5/1/2026, 11:39:24 AM		3/30/2026	HTML	HTML	TXT	
1040829	730664379	DE	001-13079	26930613	6798	1231
EX-22, EX-31.1, EX-31.2, EX-32.1	EX-101.SCH, EX-101.CAL, EX-101.DEF, EX-101.LAB, EX-101.PRE, XML	...
←
1
2
3
4
5
→
Pagination: Retrieve more than 10,000 filings
The from parameter is used to paginate through API results and is capped at 10,000. For instance, if a search query like formType:"8-K" AND ticker:AAPL matches 230 filings, but only 50 filings can be returned per API request, the from parameter can be incremented to retrieve all 230 filings. To retrieve all 230 filings:

Set "from": "0" for the first 50 filings.
Set "from": "50" for the next 50, and so on, incrementing by 50 for each subsequent request until no more filings are returned.
JSON payload example for pagination
{
    "query": "formType:\"8-K\" AND ticker:AAPL",
    "from": "0", // increase by 50 for pagination, e.g. 0, 50, 100, ...
    "size": "50",
    "sort": [{ "filedAt": { "order": "desc" }}]
}



