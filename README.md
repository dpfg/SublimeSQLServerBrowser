SublimeSQLServerBrowser
=======================
Simple tool to execute sql inside SublimeText 3. Also allow to browse database structure through the quick panel.
For now works only with MSSQL Server(throug pymssql). 

##Features

- Supports all SQL statements. 
- Show SELECT results as pretty printed table in a new tab
- Execute multiple queries separeted by GO statement
- Search tables by quick panel. Create DDL or SELECT statement for selected table.

##TODO
- Rename commands
- Move code into separate classes
- Add better connection management
- Add menu to quick panel to switch between servers
- Better status indicator
- Better error message