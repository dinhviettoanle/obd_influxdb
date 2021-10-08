# OBD : InfluxDB

## Manips de base
### Importer des données
- Installer les `requirements.txt` 
- Lancer une instance InfluxDB (`influxd` ou `<path_to_influxdb>\influxd.exe`)
- Créer un bucket `mybucket` : sur `localhost:8086` > se connecter > Data > Buckets > Create Bucket
- Modifier le fichier `basics_influx.py` : configurer les variables dans la section "Init client"
- Lancer `python ./basics_influx.py --command write_csv_sync`
- (Essayer de visualiser les données, s'il n'y a pas de résultats, essayer : `python ./basics_influx.py --command write_csv_async`)

### Visualiser toutes les données
- Aller dans Explore > Query Builder
- Entrer :
```
from(bucket: "mybucket")`<br>
  |> range(start: 0, stop: now())
```
- Submit
- (Toggle le "View Raw Data" si besoin)
