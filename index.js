const cassandra = require("cassandra-driver");

const raksix = {}

raksix.connect = (secure_path, client_id, client_secret, keyspace) => {
    return {
        client: new cassandra.Client({
            cloud: { secureConnectBundle: secure_path },
            credentials: { username: client_id, password: client_secret },
            keyspace: keyspace
        }),
        create_table: function (table, data) {
            let data_arr = []
            Object.keys(data).map(a => {
                data_arr.push(`${a} ${data[a]}`)
            })
            return this.client.execute(`CREATE TABLE ${table} (${data_arr.join()})`)
        },
        insert_into: function(table, data) {
            let data_arr = []
            let values = []
            let s = []
            Object.values(data).map(a => {
                values.push(String(a))
                s.push("?")
            })
            Object.keys(data).map(a => {
                data_arr.push(`${a}`)
            })
            return this.client.execute(`INSERT INTO ${table} (${data_arr.join()}) VALUES (${s.join()})`, values)
        },
        select: function(table) {
            return this.client.execute(`SELECT * FROM ${table}`)
        },
        find: function (table, data) {
            const key = Object.keys(data)
            const value = Object.values(data)
            if(key.length < 2 && value.length < 2){
                return this.client.execute(`SELECT * FROM ${table} WHERE ${key[0]} = ?`, [value[0]])
            }else{
                let keys_arr = []
                key.map((a, idx) => {
                    let keys_idx = key.length - 1
                    if(keys_idx > idx){
                        keys_arr.push(`${a} = ? AND`)
                    }else{
                        keys_arr.push(`${a} = ?`)
                    }
                })
                console.log(keys_arr)
                let all_keys = keys_arr.join().replaceAll(",", " ")
                return this.client.execute(`SELECT * FROM ${table} WHERE ${all_keys} ALLOW FILTERING`, ["raksix", "raksix"])
            }
        }
    }
}

module.exports = raksix