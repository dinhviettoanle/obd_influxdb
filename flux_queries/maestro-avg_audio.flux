from(bucket: "maestro_audio")
  |> range(start: 0, stop: 30)
  |> drop(columns: ["_start", "_stop"])
  |> movingAverage(n: 22050)
