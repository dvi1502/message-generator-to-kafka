
from configparser import ConfigParser

def config(filename = "kafka-producer.ini", section="common"):
    parser = ConfigParser()
    parser.read(filename)
    values = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            values[param[0]]=param[1]
    else:
        raise Exception("Section {0} is not found in the {1} file.".format(section,filename))

    return values

if __name__=="__main__":
    values = config(section="common")
    print(values)
    print(values["duration"])

    cluster = config(section="kafka")
    print(cluster.values())
