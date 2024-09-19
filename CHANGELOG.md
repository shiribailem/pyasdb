### Unstable
* (BREAKING) Changed the shelf backend from the default auto-select to dumbdbm (increases 
  portability and makes behavior more predictable. Fix by either selecting the appropriate dbm
  or converting the files manually to dumbdbm)
* Added behavior to pass a backend to use (e.g. dbm backends, or a dict variable if you want 
  to make an in-memory database)
* Added backup function for backing up live databases or for the purpose of database recovery
  (less efficient than copying files, it copies by keys, helpful for checking integrity)
* (FIX) actually enable writeback mode
* (BEHAVIOR CHANGE) entries now return an empty dictionary by default instead of raising a 
  KeyError
* Keys are now automatically convert to strings (via str())
* (BREAKING?) CSV import matching routine changed to avoid odd conversion of integers into 
  dates
* Added query_none as a special query to return *only* those entries where the field is missing or None
* Added queries submodule to provide common functions for querying
* Added compare argument to queries to support generic pre-written query functions
* (FIX) fixed bug preventing creating of new databases
* Refactored to implement true writeback with manual syncing
* Added type hinting to import_tools.csv_import
* Implemented bulk write locking
* (FIX) improved thread safety when bulk writing
* Added logging module and some debug messages
* Added indexing

### 2024.06.04
* (FIX) don't crash if dateutil is missing, making it an optional dependency
* (FIX) no longer get SyntaxError on literal_eval in CSV import

### 2024.06.03
* Added initial import_tools (CSV import currently)
* Added atExit to attempt to cleanly close the database always
* Map update, del, and contains to the underlying database
* Added built in locking for thread safety
* Implemented type checking to ensure entries are always dictionaries

### 2024.05.31
* Initial release version