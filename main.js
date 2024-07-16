import pg from "pg";
import axios from "axios";
import fs from 'fs';
import os from 'os';
import path from 'path'

const homeDir = os.homedir();
const certPath = path.join(homeDir, '.postgresql', 'root.crt');

const certData = (await axios.get('https://storage.yandexcloud.net/cloud-certs/CA.pem')).data
fs.writeFileSync(certPath, certData, 'utf-8')

const config = {
    connectionString:
        "postgres://candidate:62I8anq3cFq5GYh2u4Lh@rc1b-r21uoagjy1t7k77h.mdb.yandexcloud.net:6432/db1",
    ssl: {
        rejectUnauthorized: true,
        ca: fs.readFileSync(certPath).toString(),
    },
};

const conn = new pg.Client(config);

const data = (await axios.get('https://rickandmortyapi.com/api/character')).data.results

let reqPersList = []
for (let persData of data) {
    let reqPersData = {}
    for (let value of Object.keys(persData)) {
        if (['id', 'name', 'status', 'species', 'type', 'gender', 'origin'].includes(value)) {
            reqPersData[value] = persData[value]
        }
    }
    reqPersList.push(reqPersData)
}

async function createTableAndInsertData() {
    try {
        await conn.connect();
        console.log((await conn.query("SELECT version()")).rows[0]);
    
        const createTableQuery = `
        CREATE TABLE IF NOT EXISTS RickAndMorty (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        data JSONB NOT NULL
        );`
        await conn.query(createTableQuery);
        console.log('table creared')
    
        const comments = `
        COMMENT ON COLUMN RickAndMorty.id IS 'Первичный ключ, автоинкримент, не пустое';
        COMMENT ON COLUMN RickAndMorty.name IS 'Поле name данного персонажа из ответа сервера';
        COMMENT ON COLUMN RickAndMorty.data IS 'Полный объект для данного персонажа из ответа сервера';
        `
        await conn.query(comments);
        console.log('comments added')
    
        const insertData = `
        INSERT INTO RickAndMorty (name, data)
        VALUES ($1, $2);
        `
        for (let i = 0; i!=reqPersList.length; i++) {
            await conn.query(insertData, [reqPersList[i].name, JSON.stringify(reqPersList[i])]);
        }
        console.log('data inserted')

        const selectQuery = `
        SELECT * FROM RickAndMorty;
        `;
        const result = await conn.query(selectQuery);
        console.log('Data in table RickAndMorty:');
        console.log(result.rows);
    } catch (err) {
        console.log("ERRRRRRRRRRRRRRRROR:", err.stack)
    } finally {
        await conn.end()
    }
}

createTableAndInsertData()