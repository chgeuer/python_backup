# TODOs

- [x] Moved to proper file structure
- [x] Use python virtual environment with Python 2.7 (https://virtualenv.pypa.io/en/stable/)
- [ ] Messaging to queue missing
- [ ] Debug level command line switch
- [x] Create `ddlgen` sidecar files
- [ ] Debug password manager issue, currently not returning the proper password
- [ ] Streaming support


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
