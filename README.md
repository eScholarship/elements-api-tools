# Elements API Tools

Small programs for performing specific tasks with the Elements API. The general workflow for these is:
- Asign args and config with program_setup
- Gather and prep the data, either by input file or SQL query
- Loop the data:
    - Create the XML body for the update request
    - Send the request
    - Acknowledge 200 or print the error response.
 
## Tools

### Add Labels to Pubs
Takes an input CSV with publication IDs and lbnl-schema labels to add to them.

### Add FoR 2008 Labels to LBL Users
Queries the reporting DB, returning a set of results listing each LBL user's top 5 FoR labels, via the frequency of those labels appearing on their claimed publications. These results are then nested for processing and sent to the API.
