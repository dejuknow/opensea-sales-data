import openseautil
import dash
from dash import dcc
from dash import html
from dash.dependencies import Output, Input, State
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
from dash import dash_table
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go
import requests
import time
import pymongo
from pymongo import MongoClient
import logging
import datetime
import threading
import sys
import json
from web3 import Web3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-6s:%(lineno)d %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers = [
        logging.FileHandler('openseasalesdata.log'),
        logging.StreamHandler()
    ]
)

class NFTProject:
    def __init__(self, id, name, isArtBlocks, address, collection, totalMints, mintPrice, startingTokenNumber, startTime, baseUri, propertyElementName, noneValue, dbPrefix, useWeb3):
        self.id = id
        self.name = name
        self.isArtBlocks = isArtBlocks
        self.address = address
        self.collection = collection
        self.totalMints = totalMints
        self.mintPrice = mintPrice
        self.startingTokenNumber = startingTokenNumber
        self.startTime = startTime
        self.baseUri = baseUri
        self.propertyElementName = propertyElementName
        self.noneValue = noneValue
        self.dbPrefix = dbPrefix
        self.useWeb3 = useWeb3

        self.dbCollection = db[dbPrefix + 'Collection']
        self.dbCollection.create_index('tokenId', unique=True)

        self.dbProperties = db[dbPrefix + 'Properties']
        self.dbProperties.create_index([('projectId', 1), ('name', 1), ('value', 1)], unique=True)

    def __str__(self):
        return 'id: {0}, name: {1}'.format(self.id, self.name)

    def getTokenUri(self, tokenId):
        if self.useWeb3:
            return self.contract.functions.tokenURI(tokenId).call()
        else:
            return self.baseUri.replace('[tokenId]', str(tokenId))

nftProjects = {}

for nftProjectJson in json.load(open('projects.json')):
    # Ignore elements starting with //
    nftProjectJson = {k: v for k, v in nftProjectJson.items() if not k.startswith('//')}

    nftProject = NFTProject(**nftProjectJson)

    nftProjects[nftProject.id] = nftProject

client = MongoClient()
db = client.opensea
dbSalesCollection = db['sales']
dbSalesCollection.create_index('eventId', unique=True)
dbMetadataCollection = db['salesMetadata']

s = requests.Session()

def persistSalesDataByDateRange(projectId, address, collection, isArtBlocks, startDate, endDate):
    eventsPersistedCounter = 0
    chunkInDays = 3
    timeInterval = startDate

    while timeInterval < endDate:
        occurredAfter = timeInterval
        occurredBefore = timeInterval + chunkInDays * 86400

        if occurredBefore > endDate:
            occurredBefore = endDate

        eventsPersistedCounter += persistSalesDataByDateRangeHelper(projectId, address, collection, isArtBlocks, occurredAfter, occurredBefore)

        timeInterval = occurredBefore

    return eventsPersistedCounter

def persistSalesDataByDateRangeHelper(projectId, address, collection, isArtBlocks, occurredAfter, occurredBefore):
    logging.info('Getting sales data for timestamps ' + str(occurredAfter) + '-' + str(occurredBefore))

    eventsPersistedCounter = 0
    step = 0
    limit = 300

    while True:
        if isArtBlocks:
            querystring = {
                'collection_slug': collection,
                'event_type': 'successful',
                'only_opensea': 'true',
                'limit':str(limit),
                'offset':str(limit * step),
                'occurred_after': occurredAfter,
                'occurred_before': occurredBefore
            }
        else:
            querystring = {
                'asset_contract_address': address,
                'event_type': 'successful',
                'only_opensea': 'true',
                'limit':str(limit),
                'offset':str(limit * step),
                'occurred_after': occurredAfter,
                'occurred_before': occurredBefore
            }

        try:
            response = s.get('https://api.opensea.io/api/v1/events', params = querystring)

            if response.status_code == 429 or response.status_code == 401:
                logging.error('error: status code {}'.format(response.status_code))

                time.sleep(30)

                continue
            elif response.status_code == 400:
                logging.error('status code: ' + str(response.status_code) + '. Reached offset limit?')

                quit()
            elif response.status_code != 200:
                logging.error('status code: ' + str(response.status_code))

                continue

        except Exception as e:
            logging.error(str(e))

            continue

        responseJson = response.json()

        assetEvents = responseJson['asset_events']

        for assetEvent in assetEvents:
            if assetEvent['asset'] is None:
                continue

            if assetEvent['is_private'] == True:
                continue

            eventId = int(assetEvent['id'])
            timestamp = assetEvent['created_date']
            tokenId = int(assetEvent['asset']['token_id'])
            tokenName = assetEvent['asset']['name']
            decimals = int(assetEvent['payment_token']['decimals'])

            paymentTokenEthPriceString = assetEvent['payment_token']['eth_price']

            if paymentTokenEthPriceString is None:
                # Don't know how to calculate this payment Token
                continue

            paymentTokenEthPrice = float(paymentTokenEthPriceString)
            price = float(assetEvent['total_price']) / pow(10, decimals) * paymentTokenEthPrice
            fromAccountAddress = assetEvent['transaction']['from_account']['address']
            toAccountAddress = assetEvent['transaction']['to_account']['address']

            eventJson = {}

            eventJson['eventId'] = eventId
            if isArtBlocks:
                eventJson['projectId'] = str(int(tokenId / 1000000))
            else:
                eventJson['projectId'] = projectId
            eventJson['collection'] = collection
            eventJson['tokenId'] = tokenId
            eventJson['tokenName'] = tokenName
            eventJson['price'] = price
            try:
                eventJson['timestamp'] = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%f').replace(microsecond=0)
            except ValueError:
                eventJson['timestamp'] = datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S').replace(microsecond=0)
            eventJson['fromAccountAddress'] = fromAccountAddress
            eventJson['toAccountAddress'] = toAccountAddress

            try:
                dbSalesCollection.insert_one(eventJson)

                eventsPersistedCounter += 1
            except pymongo.errors.DuplicateKeyError:
                logging.info('Duplicate in database: ' + str(eventId))

        if len(assetEvents) == 0:
            logging.info('Persisted ' + str(eventsPersistedCounter) + ' events for timestamps ' + str(occurredAfter) + '-' + str(occurredBefore))

            return eventsPersistedCounter
        else:
            step += 1

def persistSalesData(projectId):
    nftProject = nftProjects[str(projectId)]

    eventsPersistedCounter = 0

    now = int(time.time())

    projectKey = nftProject.id

    metadata = dbMetadataCollection.find_one({'projectKey': projectKey})

    if metadata is None:
        startTime = nftProject.startTime

        if startTime == 0:
            lastFetchedDate = now - 86400 * 10
        else:
            lastFetchedDate = startTime

        dbMetadataCollection.insert_one({'projectKey': projectKey, 'lastFetchedDate': lastFetchedDate})
    else:
        lastFetchedDate = metadata['lastFetchedDate']

    logging.info('Persisting sales data. Time now = ' + str(now))

    eventsPersistedCounter += persistSalesDataByDateRange(projectId, nftProject.address, nftProject.collection, nftProject.isArtBlocks, lastFetchedDate, now)

    dbMetadataCollection.replace_one({'projectKey': projectKey, 'lastFetchedDate': lastFetchedDate}, {'projectKey': projectKey, 'lastFetchedDate': now})

    return eventsPersistedCounter

def getDataFramesByRecentCount(recentCount):
    if recentCount == 0:
        return pd.DataFrame({'timestamp': [], 'tokenId': [], 'price': []})

    results = dbSalesCollection.find().sort([('$natural', -1)]).limit(recentCount)

    return getDataFramesByDBResults(results, 1, sys.maxsize)

def getDataFramesByProjectId(projectId, startDate, endDate):
    results = dbSalesCollection.find({
        'projectId': projectId,
        'timestamp': {
            '$gte': startDate,
            '$lt': endDate
        }
    })

    return getDataFramesByDBResults(results)

def getDataFramesByDBResults(results):
    timestampColumn = []
    tokenNameColumn = []
    tokenIdColumn = []
    priceColumn = []
    urlColumn = []

    for result in results:
        tokenId = result['tokenId']

        timestampColumn.append(result['timestamp'])

        if result['projectId'].isnumeric():
            if result['projectId'] == '0':
                # Chromie Squiggle has different address
                address = '0x059edd72cd353df5106d2b9cc5ab83a52287ac3a'
            else:
                address = '0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270'
        else:
            address = nftwatcher.nftProjects[result['projectId']].address

        tokenOSUrl = openseautil.getAssetUrl(address, tokenId)

        tokenNameColumn.append('[{}]({})'.format(result['tokenName'], tokenOSUrl))
        tokenIdColumn.append(tokenId)
        priceColumn.append(result['price'])
        urlColumn.append(tokenOSUrl)

    df = pd.DataFrame(
        {
            'timestamp': timestampColumn,
            'tokenName': tokenNameColumn,
            'tokenId': tokenIdColumn,
            'price': priceColumn,
            'url': urlColumn
        }
    )

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.set_index('timestamp', drop=False)
    df = df.sort_index()

    return df

def getGraphFigure(projectId, startDate, endDate):
    df = getDataFramesByProjectId(projectId, startDate, endDate)

    maWindow = '24H'
    floorWindw = '24H'

    df['MA'] = df.price.rolling(window=maWindow).mean()
    df['FLOOR'] = df.price.rolling(window=floorWindw).min()

    fig = px.scatter(
        df,
        x='timestamp',
        y='price',
        color_continuous_scale='bluered',
        hover_data=['tokenId'],
        custom_data=("url",)
    )

    fig.update_layout(
        legend_orientation="h",
        legend_y=1.1
        )

    fig.add_trace(
        go.Scatter(
            x=df.timestamp,
            y=df.MA,
            hoverinfo='skip',
            line=dict(color='orange', width=2),
            line_shape='spline',
            name='Moving Average: ' + maWindow,
        ))

    fig.add_trace(
        go.Scatter(
            x=df.timestamp,
            y=df.FLOOR,
            # hoverinfo='skip',
            line=dict(color='green', width=2),
            line_shape='spline',
            name='Floor',
        ))

    fig.add_hline(
        y=nftProjects[projectId].mintPrice,
        line_dash='dot',
        annotation_text='mint price: ' + str(nftProjects[projectId].mintPrice),
        annotation_position='bottom right')

    return fig

load_figure_template('flatly')

app = dash.Dash(__name__, title = 'AB Sales Analytics', external_stylesheets=[dbc.themes.FLATLY])

dropdownOptions = []

for projectId, nftProject in sorted(nftProjects.items(), key = lambda k: k[0]):
    dropdownOptions.append({'label': nftProject.id + ': ' + nftProject.name, 'value': nftProject.id})

defaultNftProject = nftProjects.get(str(dropdownOptions[0]['value']))

app.layout = html.Div(children=[
    dbc.Col(
        html.H2(children='OpenSea Sales Analytics'),
        width={'offset': 1}),

    html.Div([
        dbc.Row([
            dbc.Col([
                html.H5([
                    html.Label('Project'),
                    html.A(
                        id='project-opensea-url',
                        href='javascript:void(0)',
                        target="_blank",
                        children = [
                            html.Img(
                                src=app.get_asset_url('opensea-logo.svg'),
                                height=24,
                                width=24
                            )
                        ]
                    )
                ]),
                dcc.Dropdown(
                    id='project-id-dropdown',
                    options=dropdownOptions,
                    value = dropdownOptions[0]['value'],
                    clearable=False,
                    # multi=True
                )],
            width={'size': 3, 'offset': 1}
            ),

            dbc.Col([
                html.H5('Date Range'),
                dcc.DatePickerRange(
                    id='date-picker-range',
                    number_of_months_shown=2,
                    start_date=datetime.datetime.now() + datetime.timedelta(weeks = -4),
                    end_date=datetime.datetime.now() + datetime.timedelta(days = 1)
                )],
            width={'size': 3}),

            dbc.Col([
                html.Button(
                    'Fetch Sales Events',
                    id='fetch-button',
                    className='btn btn-primary'),
                html.P(
                    id='sales-events-fetched',
                    children='')],
            width={'size': 1})
        ])

    ]),

    dbc.Row([
        dbc.Col([
            dcc.Graph(
                id='sales-graph',
                style={'height': '75vh'},
            ),
            dash_table.DataTable(
                id='sales-datatable',
                columns=[
                    {'name': 'timestamp', 'id': 'timestamp'},
                    {'name': 'tokenName', 'id': 'tokenName', 'presentation': 'markdown'},
                    {'name': 'tokenId', 'id': 'tokenId'},
                    {'name': 'price', 'id': 'price'}],
                data=[],
                page_size=20,
                sort_action='native'
            )],
            width = 10)
        ],
        justify='center'
    )
])

app.clientside_callback(
    """
    function(clicks) {
        if (clicks && 'customdata' in clicks['points'][0])
            window.open(clicks['points'][0]['customdata'][0], '_blank')

        return window.dash_clientside.no_update
    }
    """,
    Output('sales-graph', 'style'),
    Input('sales-graph', 'clickData')
)

@app.callback(
    [
        Output('sales-graph', 'figure'),
        Output('sales-events-fetched', 'children'),
        Output('project-opensea-url', 'href'),
        Output('sales-datatable', 'data'),
    ],
    [
        Input('project-id-dropdown', 'value'),
        Input('date-picker-range', 'start_date'),
        Input('date-picker-range', 'end_date'),
        Input('fetch-button', 'n_clicks')
    ])
def updateGraph(projectId, startDate, endDate, n_clicks):
    data = []
    eventsFetchedLabel = ''

    collection = nftProjects[projectId].collection

    if collection is None:
        url = 'https://opensea.io'
    else:
        url = openseautil.getCollectionUrl(collection)

    for triggered in dash.callback_context.triggered:
        if triggered['prop_id'] == 'fetch-button.n_clicks':
            eventsPersistedCounter = persistSalesData(projectId)

            eventsFetchedLabel = 'Fetched {} events'.format(eventsPersistedCounter)

            data = getDataFramesByRecentCount(eventsPersistedCounter).to_dict('records')

            break

    figure = getGraphFigure(projectId, pd.to_datetime(startDate), pd.to_datetime(endDate))

    return figure, eventsFetchedLabel, url, data

if __name__ == '__main__':
    app.run_server(debug=True)
