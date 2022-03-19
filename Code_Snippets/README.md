# Code Snippets

## Purpose
These code snippets are pulled from our running program within the `csharp-AzureBulkImport` subdirectory. They have been separated into logical files containing the appropriate methods, and each snippet is contained within a directory with an accompanying `README.md`. These READMEs will explain the high level process behind what the application code is doing, as well as provide direction to where the queries are located.

All READMEs will follow the format of `First`, `Then`, and `Finally`, and reference the filenames in the same directory as the README. Some files may be duplicated across Code Snippet examples where work was able to be repurposed. This is to ensure that the necessary files are able to be referenced in every snippet, without the need to traverse the git repository.

## Notes
* These snippets will not run in their self contained files. All functions exist within the `csharp-AzureBulkImport` subdirectory, and are commented out in preparation of our demo. This application is what was used for the bulk of our query and analysis, with exports to CSV for further visualization.

* It occurred to us quite a ways into this project that we could reuse the `RHDP` queries and pass the `typeOfMeasure` parameter as PM2.5 for those queries. This would have lessened our code and query length considerably, but would not have improved performance of our queries. We decided to maintain the pattern we had used from the beginning rather than diverging, but it is an improvement that further iterations could include.