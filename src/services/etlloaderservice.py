import os
import re
import inspect

def _get_etl_loader_list(dirname):
    files = [f.replace('.py', '') for f in os.listdir(dirname) if not f.startswith('__')]
    print(files)
    return files

def _import_etl_loaders(etlfiles):
    m = re.compile('.+ETL$', re.I)

    modules = __import__('etlloaders', fromlist=etlfiles, level=0)

    etlloaders = [(k,v) for k, v in inspect.getmembers(modules) if inspect.ismodule(v) and m.match(k)]

    classes = dict()

    for k,v in etlloaders:
        classes.update({k: v for k,v in inspect.getmembers(v) if inspect.isclass(v) and m.match(k)})

    return classes

def load(dirname):
    loaderfiles = _get_etl_loader_list(dirname)
    return _import_etl_loaders(loaderfiles)

