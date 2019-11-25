#### Env vars
```sh
undertest : when is true the LeaderElction won't start, default is false

skip.ondemandsnapshoot : when is true a periodic snapshot is produced using the value of iteration.between.snapshot, default is false

iteration.between.snapshot : by default is set to 10

max.snapshot.age : age to be consider a snapshot valid, default 600 sec

poll.timeout : value to poll topic, default 1000 

poll.timeout.unit : time unit to poll.timout default is millisec, other option is sec

poll.timeout.snapshot : value to poll snapshot topic, default value is 1

poll.timeout.unit.snapshot : : time unit to poll.timout.snapshot default is sec, other option is sec

max.snapshot.request.attempts : max number of attempt to wait a snapshot, default value is 10

namespace : namespace to use, default value is 'default'
```