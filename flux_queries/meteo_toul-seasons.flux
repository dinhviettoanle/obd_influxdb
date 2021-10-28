avgDay = (tables=<-, startDate, endDate, resultName) =>
  tables
    |> range(start: time(v : startDate), stop: time(v : endDate))
    |> filter(fn: (r) => r.station == "12-toulouse-montaudran")
    |> filter(fn: (r) => r._field == "temperature") 
    |> aggregateWindow(every: 1h, fn: mean) 
    |> window(every: 1d)
    |> stateDuration(
          fn: (r) => true,
          column: "timeDiff",
          unit: 1ns
        )
    |> map(fn: (r) =>
          ({ r with _time: time(v: r.timeDiff) })
        )
    |> drop(columns: ["timeDiff"])
    |> group(columns: ["_time", "station"])
    |> mean()
    |> group(columns: ["station"])
    |> yield(name : resultName)



from(bucket: "meteo_toulouse") |> avgDay(startDate: "2021-06-21", endDate: "2021-09-21", resultName: "summer_21")
from(bucket: "meteo_toulouse") |> avgDay(startDate: "2020-06-21", endDate: "2020-09-21", resultName: "summer_20")
from(bucket: "meteo_toulouse") |> avgDay(startDate: "2019-06-21", endDate: "2019-09-21", resultName: "summer_19")


from(bucket: "meteo_toulouse") |> avgDay(startDate: "2021-03-21", endDate: "2021-06-21", resultName: "spring_21")
from(bucket: "meteo_toulouse") |> avgDay(startDate: "2020-03-21", endDate: "2020-06-21", resultName: "spring_20")

from(bucket: "meteo_toulouse") |> avgDay(startDate: "2020-12-21", endDate: "2021-03-21", resultName: "winter_20_21")
from(bucket: "meteo_toulouse") |> avgDay(startDate: "2019-12-21", endDate: "2020-03-21", resultName: "winter_19_20")

from(bucket: "meteo_toulouse") |> avgDay(startDate: "2020-09-21", endDate: "2020-12-21", resultName: "autumn_20")
from(bucket: "meteo_toulouse") |> avgDay(startDate: "2019-09-21", endDate: "2019-12-21", resultName: "autumn_19")