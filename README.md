## Design
There are several questions which are not clear in the document, so I'll make below assumptions:
- how to identify different applications? I'll use the website field as the key, so when user upload a new metadata for the same website, it will override the old metadata. 
- what search query language is supported? Ideally we should use ElasticSearch to implement the persistence layer and support very flexible query language. But here we don't want to use any existing DB or storage service. So I just implemented a very rough query support. 

The whole implementation is more focused on engineering practice and code quality, with a reasonable project structure which took one or two days to deliver from scratch. if the exercise is trying to find the best algorithm to implement a ElasticSearch service, then I probably went wrong way :).

Basically this metadata service contains two parts:
- gRPC gateway based on a protobuf definition to provide RESTful API
- a simple in memory storage implementation to provide persistence layer

# Build

### Build binary

```
$ make build
```

# API code generation
After making API change in .proto file, run following command
```
$ make proto-gen
```

## Test

* Run unit tests

```
$ make test
```

* Run real test

On macbook after finish the build you can run `bin/darwin/appmeta` to start the service, then use the clitool to test.
```
bin/darwin/clitool --url http://127.0.0.1:3000 upload -f test/files/app1.yml
bin/darwin/clitool --url http://127.0.0.1:3000 upload -f test/files/app2.yml
bin/darwin/clitool --url http://127.0.0.1:3000 upload -f test/files/app3.yml
bin/darwin/clitool --url http://127.0.0.1:3000 upload -f test/files/app4.yml

bin/darwin/clitool --url http://127.0.0.1:3000 search -q "maintainers.email = firstmaintainer@hotmail.com"
bin/darwin/clitool --url http://127.0.0.1:3000 search -q "first"
```
