# raksix cassandra-driver

raksix-cassandra easier than cassandra-driver

```
const cassandra = require("raksix-cassandra")

// connect config payload
const config = {
    secure_path: "./secure-zip-path",
    client_id: "client_id",
    client_secret: "client_secret",
    keyspace: "keyspace"
}

// connect cassandra-driver
const connection = cassandra.connect(config.secure_path, config.client_id, config.client_secret, config.keyspace)

// table payload
const data2 = {
    id: "text PRIMARY KEY",
    name: "text",
    email: "text",
    password: "text"
}


// create table method
connection.create_table("test", data2)


// insert payload
const data3 = {
    id: "1",
    name: "furkan",
    email: "furkan@gmail.com",
    password: "1234"
}


// insert method
connection.instert("test", data3)


// find method
connection.find("test", { name: "furkan" }).then(rs => {
    // result
    console.log(rs.rows)
})
// find all
connection.find_all("test").then(rs => {
    //  result
    console.log(rs)
})

// update method
connection.update("test", { name: "furkan" }, { email: "raksix@raksix.wtf", password: "12346" })
```
