# SwayDB.stress

Stress testing [SwayDB](https://github.com/simerplaha/SwayDB).

http://swaydb.io/

# SimulationSpec
10 User [Actors](https://doc.akka.io/docs/akka/2.5.4/scala/typed.html) 
concurrently & repeatedly perform CRUD operations for a specified period of time by 
- creating batches of 10000 products
- updating & deleting random products 
- asserting the expected state of the User's products.

# WeatherDataSpec
Concurrently writes multiple WeatherData entries as single put operations and batch operations and then 
concurrently reads the inserted data by performing various [Read API](http://swaydb.io#api/read-api) 
operations.