# SwayDB.stress

Stress testing [SwayDB](https://github.com/simerplaha/SwayDB).

http://swaydb.io/

# SimulationSpec
Multiple User [Actors](https://doc.akka.io/docs/akka/2.5.4/scala/typed.html) 
concurrently & repeatedly perform CRUD operations for a specified period of time by 
- creating 1000 products repeatedly with [put](http://www.swaydb.io/#api/write-api/put) 
                                    and [batchPut](http://www.swaydb.io/#api/write-api/batchPut).
- updating & deleting random products with [put](http://www.swaydb.io/#api/write-api/put),  
                                       [batchPut](http://www.swaydb.io/#api/write-api/batchPut),
                                       [remove](http://www.swaydb.io/#api/write-api/remove) & 
                                       [batchRemove](http://www.swaydb.io/#api/write-api/batchRemove)
- asserting the expected state of the User's products

This test was successfully run for **6 hours** with 100 concurrent Users performing CRUD 
operations resulting in a 15 GB database with 500 million entries.

#### Run this test

```scala
sbt clean 'testOnly simulation.SimulationSpec'
```
Then follow the prompts in the console

```
Select number of concurrent Users (hit Enter for 100):

How many minutes to run the test for (hit Enter for 10 minutes): 
```

# WeatherDataSpec
Concurrently writes 1 million WeatherData entries as single put operations and batch operations and then 
concurrently reads the inserted data by performing various [Read API](http://swaydb.io#api/read-api) 
operations multiple times.

APIs used in this tests are -  
- put
- batch
- batch random
- get
- foldLeft
- foreach
- takeWhile
- head & last
- mapRight
- take
- drop
- takeRight
- count
- remove