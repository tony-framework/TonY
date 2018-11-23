import xml.etree.ElementTree as ET
import sys

# This script parses tony-site.xml, then returns a string that
# includes all the necessary flags for Play to consume.
# Returns empty string if there's an unrecognizable key in
# tony-site.xml

if (len(sys.argv) < 2):
    print("Usage: python xml_to_play_opts.py <path/to/tony-site.xml>")
    sys.exit(1)

tree = ET.parse(sys.argv[1])
root = tree.getroot()

options = ""
for elem in root:
    if elem[0].text == "tony.history.location" \
            or elem[0].text == "tony.keytab.user" \
            or elem[0].text == "tony.keytab.location":
        options += "-D" + elem[0].text + "=" + elem[1].text + " "
    elif elem[0].text == "tony.https.port":
        options += "-Dplay.server.https.port=" + elem[1].text + " "
    elif elem[0].text == "tony.https.keystore.path":
        options += "-Dplay.server.https.keyStore.path=" + elem[1].text + " "
    elif elem[0].text == "tony.https.keystore.type":
        options += "-Dplay.server.https.keyStore.type=" + elem[1].text + " "
    elif elem[0].text == "tony.https.keystore.password":
        options += "-Dplay.server.https.keyStore.password=" + elem[1].text + " "
    elif elem[0].text == "tony.https.keystore.algorithm":
        options += "-Dplay.server.https.keyStore.algorithm=" + elem[1].text + " "
    elif elem[0].text == "tony.http.port":
        options += "-Dhttp.port=" + elem[1].text + " "
    elif elem[0].text == "tony.secret.key":
        options += "-Dplay.http.secret.key=" + elem[1].text + " "
    elif elem[0].text == "tony.init.module":
        options += "-Dplay.modules.enabled+=" + elem[1].text + " "
    else:
        options = ""
        print("Invalid option: " + elem[0].text)
        break

if options == "":
    print("Invalid properties in tony-site.xml")

print(options)
sys.exit(0)