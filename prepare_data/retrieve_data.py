import urllib.request

list_years = [1979, 1989, 1999, 2009, 2019]

for year in list_years:
    data = {"stns": "251:260:350", "TEMP": "SUNR:PRCP", "byear": year, "bmonth": "7",
            "bday": "1", "eyear": year, "emonth": "7", "eday": "31"}

    params = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url='http://projects.knmi.nl/klimatologie/daggegevens/getdata_dag.cgi')

    with urllib.request.urlopen(req, data=params) as response:
        resp = response.read()
        # print(resp)
        f = open("../weather_files/weather_" + str(year) + ".csv", "w")
        f.write(resp.decode("utf-8"))
        f.close()
