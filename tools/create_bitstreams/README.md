### You can upload some files into items using `create_bitstreams.py` script.
It will create/update 3 Items:
1. Upload 100 files (copies of the `template.png` file)
2. Upload ZIP file (zipped `template.png` file)
3. Upload big file (3GB)
4. All files will be deleted except the `template.png` file.

If the running DSpace doesn't have three items, the community and collection will be created. 
Subsequently, the items will be created there.

How to run the script:
- Run `pip install -r requirements.txt`
- Update `project_settings.py`
- Run `python create_bitstreams.py`

### Override endpoint
If there is env DSPACE_REST_API, it is used as `env.backend.endpoint`.
