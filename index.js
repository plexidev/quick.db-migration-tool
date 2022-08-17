#!/usr/bin/env node
const fs = require("fs");
const sqlite3 = require("sqlite3");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");

const vargs = yargs(hideBin(process.argv))
    .option("input", {
        alias: "i",
        type: "string",
        description: "The input sqlite file to fix"
    })
    .option("output", {
        alias: "o",
        type: "string",
        description: "The output sqlite file"
    })
    .parse()

function fatal(msg) {
    console.log(msg);
    process.exit(1);
}

function getTables(db) {
    return new Promise((res) => {
        const tables = [];
        db.each("SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'", (err, row) => {
            if (err) {
                fatal(err);
            }

            tables.push(row.name);
        }, () => {
            res(tables);
        });
    });
}

function processTable(db, dbOut, table) {
    return new Promise(res => {
        console.log(`Processing table: ${table}`);
        dbOut.run(`CREATE TABLE ${table} (ID TEXT, json TEXT)`, error => {
            if (error) {
                fatal(error);
            }

            const stmt = dbOut.prepare(`INSERT INTO ${table} VALUES (?, ?)`, err => {
                if (err) {
                    fatal(err);
                }

                db.each(`SELECT ID, json FROM ${table}`, (err, row) => {
                    if (err) {
                        fatal(err);
                    }

                    console.log(`Processing row with key ${row.ID}`);
                    const toInsert = JSON.parse(JSON.parse(row.json));
                    stmt.run(row.ID, toInsert);
                }, () => {
                    stmt.finalize(err => {
                        if (err) {
                            fatal(err);
                        }

                        res();
                    });
                });
            });
        });
    });
}

async function main() {
    if (!vargs.input) {
        fatal("Missing input");
    }

    if (!vargs.output) {
        fatal("Missing output");
    }

    if (vargs.input == vargs.output) {
        fatal("Output cannot be the same as input");
    }

    if (!fs.existsSync(vargs.input)) {
        fatal(`${vargs.input}: file doesn't exist`);
    }

    // if (fs.existsSync(vargs.output)) {
    //     fatal(`output file already exist: ${vargs.output}`);
    // }

    console.log(`Loading ${vargs.input} file`);
    const db = new sqlite3.Database(vargs.input);
    console.log("Sqlite loaded");

    console.log(`Creating output sqlite file: ${vargs.output}`);
    const dbOut = new sqlite3.Database(vargs.output);

    console.log("Getting tables");
    const tables = await getTables(db);
    console.log(`Tables found: [${tables.join(", ")}]`);

    for (const table of tables) {
        await processTable(db, dbOut, table);
    }

    console.log("Done!");
    db.close();
    dbOut.close();
}


main();
