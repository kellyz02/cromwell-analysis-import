# cromwell-analysis-import
Based on the manual cleanup process created by Jenna Liebe,
the automation seeks to save time and reduce errors. The
automation was developed with JavaScript using Azure
Functions, which makes it serverless. It consists of one “Function
App”, <redacted>, which groups two
functions together. The first function,
CromwellSuccessReceiver, finds/creates the Atlas object(s)
and copies files from Cromwell into the Atlas and Cromwell
Outputs storage accounts. The second function,
CromwellCleanupDeletion, checks on the copy progress of
files to Atlas and once done, copies those files into the Atlas
Dropbox, and deletes all source files. The automation only cleans
up files for succeeded runs for workflow types that have been
included in the automation code.

## Flowchart depiction of how the automation works: 
![Untitled-2024-02-06-1447](https://github.com/kellyz02/cromwell-analysis-import/assets/108567735/3910434d-a242-4799-95d7-89a1a9b0ec66)
