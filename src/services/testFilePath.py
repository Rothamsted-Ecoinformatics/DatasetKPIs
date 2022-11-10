from os.path import exists

def TestRegistryPath():
    return exists('src/registry.json')