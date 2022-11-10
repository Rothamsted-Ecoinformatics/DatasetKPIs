from os.path import exists
import config

def TestRegistryPath():
    return exists('src/registry.json')

def TestRegistryPathConfig():
    return exists(config.registryPath)