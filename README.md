# am-2-atom-metadata-migrator
*Migrate Archivematica AIP metadata to existing AtoM descriptions, without DIP upload*

These scripts are a tool to automate the migration of Archivematica metadata in scenarios where access copies already exist in AtoM. Using the existing DIP Upload functionality, it is currently not possible to add Archivematica metadata without creating new AtoM descriptions.

This tool reads data from the Archivematica Elasticsearch index and inserts it into the AtoM MySQL database. It currently uses a match between Archivematica AIP file filenames and AtoM digital object filenames. A future version could implement more complex heuristics to make a match.

* Move the 'am-es-query.py' script to the server that hosts your Archivematica Elasticsearch server.
* If needed, change the server address at the top of the script.
* Create a Python virtual environment or run using your native Python installation (both 2.x and 3.x will work).
* Run `pip install elasticsearch`.
* Run `python am-es-query.py`.
* The script will query the Elasticsearch index and retrieve all relevant AIP files metadata.
* The script will write this information to a Sqlite database that is named after the UUID of the pipeline that this Elasticsearch server resides on.
* The Sqlite database records the date that this data was written. If the script is run again in the future, it will only add the AIP files that were created on that pipeline since the last time the script was run.
* The theoretical maximum number of rows in a Sqlite table is 18,446,744,073,709,551,616. This limit is unreachable since the maximum database size of 140 terabytes will be reached first. See https://sqlite.org/limits.html.
* The Sqlite database will be created in the same directory as the 'am-es-query.py' script.
* The Sqlite database consists of a single file. Move it to server that hosts your AtoM MySQL server along with the 'atom-sql-insert-.py' script.
* Create a Python virtual environment or run using your native Python installation (both 2.x and 3.x will work).
* Update the 'atom-sql-insert.py' script with your AtoM site URL, AtoM MySQL login information, and the name of your Sqlite database.
* Run `pip install pymysql`
* Run `python atom-sql-insert.py`
* The script will look for matches between AIP file filenames and AtoM digital object filenames. When it finds a match, it will insert the metadata from the AIP file into the AtoM digital object description (AIP UUID, Object UUID, file format, file format version, PRONOM identifier).
* The script will update the Sqlite database to record all matches that were made, along with their AtoM permalink slugs.
* It will also record all those Archivematica AIP files for which a match was not found or if the matching process had a failure.
* The script can be run at subsequent times, after it has been update with more Archivematica Elasticsearch data or after updates to the AtoM site. It will skip AIP files and AtoM descriptions that have already been matched.
* A future version of this tool may provide a GUI to the Sqlite information. In the meanwhile, you can use the 'DB Browser for Sqlite' tool to browse the Sqlite database contents and track the Archivematica to AtoM matches. See https://sqlitebrowser.org/.
