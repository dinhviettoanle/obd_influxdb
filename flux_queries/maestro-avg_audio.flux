// from(bucket: "maestro_audio")
//   |> range(start: 0, stop: 30)
//   |> drop(columns: ["_start", "_stop"])
//   |> movingAverage(n: 22050)


import "math"

from(bucket: "maestro_audio")
  |> range(start: 0, stop: 30)
  |> drop(columns: ["_start", "_stop"])
  |> map(fn: (r) => ({ r with _value: math.abs(x: 3.0 * r._value) }))
  |> timedMovingAverage(every: 20ms, period: 50ms)
  // |> timedMovingAverage(every: 20ms, period: 5ms)
  // |> movingAverage(n: 5512)
  |> yield(name: "avg")