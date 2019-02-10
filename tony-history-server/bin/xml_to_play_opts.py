#!/export/apps/python/2.7/bin/python

# This script parses tony-site.xml, then returns a string that
# includes all the necessary flags for Play to consume.

import xml.etree.ElementTree as ET
import sys


TONY_TO_PLAY_MAPPINGS = {
    'tony.https.port': 'play.server.https.port',
    'tony.https.keystore.path': 'play.server.https.keyStore.path',
    'tony.https.keystore.type': 'play.server.https.keyStore.type',
    'tony.https.keystore.password': 'play.server.https.keyStore.password',
    'tony.https.keystore.algorithm': 'play.server.https.keyStore.algorithm',
    'tony.http.port': 'http.port',
    'tony.secret.key': 'play.http.secret.key',
}

if len(sys.argv) < 2:
    print("Usage: python xml_to_play_opts.py <path/to/tony-site.xml>")
    sys.exit(1)

tree = ET.parse(sys.argv[1])
root = tree.getroot()

# merge XIncluded file properties
xincludes = root.findall('{http://www.w3.org/2001/XInclude}include')
for xinclude in xincludes:
    included_file = xinclude.get('href')
    data = ET.parse(included_file).getroot()
    root.extend(data)

# construct Play options
play_opts = ''
nodes = root.findall('property')
for node in nodes:
    name = node.find('name').text
    value = node.find('value').text
    if name in TONY_TO_PLAY_MAPPINGS:
        name = TONY_TO_PLAY_MAPPINGS[name]
    play_opts += '-D' + name + '=' + value + ' '

print(play_opts)