# How to put translations from excel into json
This package servers to transform exported data from excel to json, usable in dspace

1. Copy data from excel to new Excel file
   - keep just the keys and values (see `cs.csv` for example)
2. Save as comma separated values, utf-8 encoded. (!IMPORTANT!)
3. Check the new saved file
   - delete any redundant newlines (it is possible to search and replace marked part in any IDE, usually it was enter and then ")
   - python requires each line to contain key.name, "key value"
4. Name the file `cs.csv`
5. Run the `csv_to_json.py` script
6. Output json can is created as `out.json`
   - check the file for any visible mistakes. Usually first char of first
key is some weird special character, just delete it
   - it should be valid json, you can consider checking if it 
abides by the json rules
7. Copy the result to appropriate place in dspace repository
8. Have a nice day
