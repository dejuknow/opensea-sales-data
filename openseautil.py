import configparser

scriptPath = '/Users/dennisju/dev/nft-watcher/'
config = configparser.ConfigParser()
config.read(scriptPath + 'config.ini')
apiKeys = config['OPENSEA']['apiKeys'].split(',')
g_apiKeyIndex = 0

def getAssetUrl(address, tokenId):
    return 'https://opensea.io/assets/{}/{}'.format(address, tokenId)

def getABCollectionUrl(collection, name):
    allNames = 'All%20' + name

    if allNames[-1] != 's':
        allNames = allNames + 's'

    return 'https://opensea.io/collection/{}?search[sortAscending]=true&search[sortBy]=PRICE&search[stringTraits][0][name]={}&search[stringTraits][0][values][0]={}&search[toggles][0]=BUY_NOW'.format(collection, name, allNames)

def getCollectionUrl(collection):
    return 'https://opensea.io/collection/{}?search[sortAscending]=true&search[sortBy]=PRICE&search[toggles][0]=BUY_NOW'.format(collection)
