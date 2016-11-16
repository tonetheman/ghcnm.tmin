
import time

def read_raw_file():
    start_time = time.time()
    inf = open("ghcnm.tmin.v3.3.0.20161112.qcu.dat","r")

    countries = {}

    count = 0

    def get_value(line,start_pos):
        value = line[start_pos:start_pos+5]
        dm = line[start_pos+5:start_pos+6]
        qc = line[start_pos+6:start_pos+7]
        ds = line[start_pos+7:start_pos+8]
        return (value,dm,qc,ds)

    for line in inf:
        country_code = line[0:3]
        wmo = line[3:7]
        wmo2 = line[8:10]
        year = line[11:15]
        element = line[15:19]
        # value1 = line[19:24]
        # dmflag1 = line[24:25]
        # qcflag1 = line[25:26]
        # dsflag1 = line[26:27]
        (value1,dmflag1,qcflag1,dsflag1) = get_value(line,19)
        (value2,dmflag2,qcflag2,dsflag2) = get_value(line,27)
        (value3,dmflag3,qcflag3,dsflag3) = get_value(line,35)
        (value4,dmflag4,qcflag4,dsflag4) = get_value(line,43)
        (value5,dmflag5,qcflag5,dsflag5) = get_value(line,51)
        (value6,dmflag6,qcflag6,dsflag6) = get_value(line,59)
        (value7,dmflag7,qcflag7,dsflag7) = get_value(line,67)
        (value8,dmflag8,qcflag8,dsflag8) = get_value(line,75)
        (value9,dmflag9,qcflag9,dsflag9) = get_value(line,83)
        (value10,dmflag10,qcflag10,dsflag10) = get_value(line,91)
        (value11,dmflag11,qcflag11,dsflag11) = get_value(line,99)
        (value12,dmflag12,qcflag12,dsflag12) = get_value(line,107)

        if countries.has_key(country_code):
            tmp = countries[country_code]
            tmp = tmp + 1
            countries[country_code] = tmp
        else:
            countries[country_code] = 1

        count = count + 1

    inf.close()
    stop_time = time.time()

    print "----------------------------"
    print "start",start_time, "stop", stop_time, "diff", (stop_time-start_time)
    print "lines", count
    # print countries

def create_db():
    import sqlite3
    db = sqlite3.connect("data.db")
    sql = """CREATE TABLE data (country_code, wmo, wmo2, year, element,
        value1,dmflag1,qcflag1,dsflag1,
        value2,dmflag2,qcflag2,dsflag2,
        value3,dmflag3,qcflag3,dsflag3,
        value4,dmflag4,qcflag4,dsflag4,
        value5,dmflag5,qcflag5,dsflag5,
        value6,dmflag6,qcflag6,dsflag6,
        value7,dmflag7,qcflag7,dsflag7,
        value8,dmflag8,qcflag8,dsflag8,
        value9,dmflag9,qcflag9,dsflag9,
        value10,dmflag10,qcflag10,dsflag10,
        value11,dmflag11,qcflag11,dsflag11,
        value12,dmflag12,qcflag12,dsflag12
        )
    """
    cur = db.cursor()
    cur.execute(sql)
    db.commit()

read_raw_file()
