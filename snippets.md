#print(os.path.abspath(sys.argv[0]))
#txt = "Nobody inspects the spammish repetition"
#print(hashlib.md5(txt.encode('utf-8')).hexdigest())
#print(base64.standard_b64encode(hashlib.md5(txt.encode('utf-8')).digest()).decode("utf-8") )

https://docs.python.org/2/distutils/setupscript.html
http://docs.python-guide.org/en/latest/shipping/packaging/
https://pythonhosted.org/an_example_pypi_project/setuptools.html
https://packaging.python.org/tutorials/managing-dependencies/
https://www.safaribooksonline.com/library/view/the-quick-python/9781935182207/kindle_split_033.html



### `requirements.txt`

Run `pip install -r requirements.txt` to install dependencies.

```txt
pid>=2.2.0
azure-storage-blob>=1.1.0
```
