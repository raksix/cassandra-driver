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
        instert: function (table, data) {
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
        find_all: function (table, options) {
            let ops = {
                limit: 5000
            }
            if (options) {
                if (options.limit) {
                    ops.limit = options.limit
                }
            }
            return this.client.execute(`SELECT * FROM ${table}`, [], {
                fetchSize: ops.limit,
            })
        },
        find: function (table, data, options) {
            let ops = {
                limit: 5000
            }
            if (options) {
                if (options.limit) {
                    ops.limit = options.limit
                }
            }
            const key = Object.keys(data)
            const value = Object.values(data)
            if (key.length < 2 && value.length < 2) {
                return this.client.execute(`SELECT * FROM ${table} WHERE ${key[0]} = ? ALLOW FILTERING`, [value[0]], {
                    fetchSize: ops.limit
                })
            } else {
                let keys_arr = []
                key.map((a, idx) => {
                    let keys_idx = key.length - 1
                    if (keys_idx > idx) {
                        keys_arr.push(`${a} = ? AND`)
                    } else {
                        keys_arr.push(`${a} = ?`)
                    }
                })
                let all_keys = keys_arr.join().replaceAll(",", " ")
                return this.client.execute(`SELECT * FROM ${table} WHERE ${all_keys} ALLOW FILTERING`, value, {
                    fetchSize: ops.limit
                })
            }
        },
        update: function (table, find, data) {
            const Dkey = Object.keys(data)
            const Dvalue = Object.values(data)
            const key = Object.keys(find)
            const value = Object.values(find)
            const update_arr = []
            const keys_arr = []
            const this_arr = []
            Dkey.map(a => {
                update_arr.push(data[a])
                this_arr.push(`${a} = ?`)
            })

            key.map((a, idx) => {
                let keys_idx = key.length - 1
                update_arr.push(find[a])
                if (keys_idx > idx) {
                    keys_arr.push(`${a} = ? AND`)
                } else {
                    keys_arr.push(`${a} = ?`)
                }
            })

            let all_keys = keys_arr.join().replaceAll(",", " ")
            const this_set = this_arr.join()
            return this.client.execute(`UPDATE ${table} SET ${this_set} WHERE ${all_keys}`, update_arr)
        }
    }
}

module.exports = raksix
