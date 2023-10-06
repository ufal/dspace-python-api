# Check of correct import of resource policies
### We check ONLY if ANONYMOUS group has same READ access to ITEMS in Dspace5 and Dspace7 after success data import! We don't check other resource policies.

## How to use it:
1. Update `const.py`
   - `use_ssl = False`
   - `host = "<YOUR_SERVER>"`
   - `fe_port = "<YOUR_FE_PORT>"`
   - `be_port = "<YOUR_BE_PORT>"`
   - `be_location = "/server/"`
   
   - `authentication = False` - we test anonymous access

   - `CLARIN_DSPACE_NAME = "clarin-dspace"`
   - `CLARIN_DSPACE_HOST = "localhost"`
   - `CLARIN_DSPACE_USER = "<USERNAME>"`
   - `CLARIN_DSPACE_PASSWORD = "<PASSWORD>"`

2. Be sure your project contains files:
   **IMPORTANT:** If `data` or `temp-files` folders don't exist in the project, create them
   - `temp-files/item_dict.json` - dict of mapping item IDs from Dspace5 to Dspace7
   - `data/handle.json` - data of handles from Dspace5

3. Run resource policy checker for anonymous view of items in Dspace7 based on Dspace5 resource policcies
   - **NOTE:** database must be full
   - **NOTE:** item_dict.json has to contain actual IDs from database of Dspace5 mapping to IDs of Dspace7
   - **NOTE:** dspace server must be running
   - From the `dspace-python-api/data_checker` run command `python main.resource_policy_pump.py`

4. Check `logs.log` in `data_checker` for `Resource policies checker of anonymous view of items`

    
    