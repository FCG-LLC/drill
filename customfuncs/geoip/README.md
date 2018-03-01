# drill-geoip

Naive implementation of GeoIP drill custom function.

## Compilation

`mvn clean compile assembly:single package`

## Installation

```
sudo echo "drill.logical.function.package+=[cs.drill.geoip]" >> drill/conf/drill-override.conf
```

```
sudo cp target/drill-geoip-1.1-drill.jar /opt/drill/jars/3rdparty/
```

## Usage

```
SELECT foo, bar, geoip_city(src_ip1, src_ip2), geoip_country(src_ip1, src_ip2) FROM sometable
```

## Populating custom (local) geo ip information

Populating custom entries for enrichment is being done via toucan.

All one need to do is to provide entry to toucan config section which follows this pattern (this is actual snippet from .yaml file which can be put in `/etc/collective-sense/toucan/` directory):

```
key: drill/geoip_enrichment_user
value:
  - subnet: "10.3.3.1/24"
    city: "Krakow"
    country: "Krakow Office"
    lon: 19.945
    lat: 50.0657
  - subnet: "10.12.1.0/24"
    city: "Fremont"
    country: "Hurricane Electric"
    lon: -121.9886
    lat: 37.5483
  - subnet: "10.12.2.1/24"
    city: "San Ramon"
    country: "San Ramon Office"
    lon: -121.978
    lat: 37.7799
```

Then Drill will check for updates every 5 minutes.
