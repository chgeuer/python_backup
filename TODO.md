# TODOs

- [x] Moved to proper file structure
- [x] Use python virtual environment with Python 2.7 (https://virtualenv.pypa.io/en/stable/)
- [ ] Messaging to queue missing
- [ ] Debug level command line switch
- [x] Create `ddlgen` sidecar files
- [ ] Debug password manager issue, currently not returning the proper password
- [x] Streaming support
- [ ] read/parse ASE dump history file and check precise timestamps
- [ ] check with ASE eng. When ddlgen SQL file from full backup time contains certain DB file sizes, and transactions grew the DB size, how does restore handle that? 
- [ ] validate that script works with Azure Storage Immutable Storage (WORM) feature. 
- [ ] Monitoring / web hook --> Sebastian provide information how to notify





full   140000    needed for restore
tran   141000    needed for restore
tran   142000    needed for restore
tran   145000    needed for restore
       145959                            restore point
full   150000    not needed
tran   150100    needed for restore
tran   150200    not needed


full   140000    needed for restore
tran   141000    needed for restore
tran   142000    needed for restore
tran   145000    needed for restore
tran   150100    needed for restore










## snippets

```python

https://azure.github.io/azure-storage-python/ref/azure.storage.blob.blockblobservice.html
put_block(container_name, blob_name, block, block_id, validate_content=False)
put_block_list(container_name, blob_name, block_list, validate_content=False)

 
#!/usr/bin/python2.7

import os
import io
import errno, time

os.mkfifo("1.pipe")
pipe = os.open("1.pipe", os.O_RDONLY) 
    while 1:
        try:
            input = os.read(pipe, 8)
        except OSError as err:
            if err.errno == 11:
                continue
            else:
                print(err)
                raise err
        if input:
            print("Data: {input}".format(input=input))





------------------------------------

#!/usr/bin/python2.7

import os
import io
import errno, time

os.mkfifo("1.pipe")
pipe = os.open("1.pipe", os.O_RDONLY)
while 1:
    try:
        input = os.read(pipe, 8)
    except OSError as err:
        if err.errno == 11:
            continue
        else:
            print(err)
            raise err
    if input:
        print("Data: {input}".format(input=input))

os.close(pipe)
```
