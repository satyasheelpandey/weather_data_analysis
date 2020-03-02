import urllib.request\
# url = 'http://projects.knmi.nl/klimatologie/daggegevens/getdata_dag.cgi'


# params = urllib.parse.urlencode('stns=251:260:350&TEMP:SUNR:PRCP&byear=2019&bmonth=7&bday=1&eyear=2019&emonth=7&eday=31')
#
#
f = urllib.request.urlopen('http://projects.knmi.nl/klimatologie/daggegevens/getdata_dag.cgi')
# #
req = urllib.request.Request(url='http://projects.knmi.nl/klimatologie/daggegevens/getdata_dag.cgi',
                             data={"stns":"251:260:350","TEMP":"SUNR:PRCP","byear":"2019","bmonth":"7","bday":"1","eyear":"2019" ,"emonth":"7","eday":"31")
#
f = urllib.request.urlopen(req)
print(f.read())
