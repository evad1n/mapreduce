# MapReduce - Distributed Batch Processing

Based on [this paper](http://research.google.com/archive/mapreduce.html)

## CLI Usage


```
    go build -o client.exe
    ./client.exe
```

For the master 

```
    ./client.exe -master <INPUT_DB> <OUTPUT_DB>
```

```                                                        
  -M int                                                                              
        Number of map tasks (default 10)                                              
  -R int                                                                              
        Number of reduce tasks (default 10)                                           
  -address string                                                                     
        Address of the master node (default "localhost:8080")                         
  -master                                                                             
        Whether this node is the master or a worker                                   
  -mode string                                                                        
        (part1|part2|main) For testing (default "main")                               
  -port string                                                                        
        The port to listen on (default "8080")                                        
  -tempdir string                                                                     
        The directory to store temporary files in (default "tmp/mapreduce.47238")     
  -wait                                                                               
        Should workers wait for a master signal (keypress) or start immediately upon joining
```
