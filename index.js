#!/usr/bin/env node
const fs = require("fs");
const Database = require("better-sqlite3");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");

const vargs = yargs(hideBin(process.argv))
    .option("input", {
        alias: "i",
        type: "string",
        description: "The input sqlite file to fix",
    })
    .option("output", {
        alias: "o",
        type: "string",
        description: "The output sqlite file",
    })
    .option("check-integrity", {
        alias: "c",
        type: "boolean",
        description: "Check the integrity of the sqlite file",
    })
    .parse();

function fatal(msg) {
    console.log(msg);
    process.exit(1);
}

function getTables(db) {
    const query = "SELECT name FROM sqlite_master WHERE type='table'";
    const statement = db.prepare(query);
    const tables = statement.all();
    return tables.map((table) => table.name);
}

function processTable(srcDb, destDb, tableName) {
    console.log(`Processing table: ${tableName}`);
    const schema = srcDb
        .prepare(`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`)
        .get(tableName);

    // Create the table in the destination database with the same schema
    destDb.exec(schema.sql);

    // Prepare SELECT and INSERT statements
    const selectStmt = srcDb.prepare(`SELECT * FROM ${tableName}`);
    const columnNames = selectStmt.columns().map((column) => column.name);
    const insertStmt = destDb.prepare(
        `INSERT INTO ${tableName} (${columnNames.join(
            ", "
        )}) VALUES (${columnNames.map(() => "?").join(", ")})`
    );

    // Begin a transaction in the destination database
    const insertTransaction = destDb.transaction(() => {
        for (const row of selectStmt.iterate()) {
            console.log(`Inserting row with ID ${row.ID}`);
            let toInsert = row.json;
            let oldInsert = row.json;
            while (true) {
                try {
                    const tmp = JSON.parse(toInsert);
                    if (typeof tmp == "object" || Array.isArray(tmp)) {
                        break;
                    } else if (typeof tmp == "string") {
                        oldInsert = toInsert;
                        toInsert = tmp;
                    } else if (typeof tmp == "number") {
                        if (tmp > Number.MAX_SAFE_INTEGER) {
                            // restore because it's too big
                            toInsert = oldInsert;
                        }
                        break;
                    } else if (typeof tmp == "boolean") {
                        break;
                    } else {
                        fatal(`Unknown type: ${typeof tmp}`);
                    }
                } catch (e) {
                    // restore last previous string
                    toInsert = oldInsert;
                    break;
                }
            }

            if (typeof toInsert == "number" || typeof toInsert == "string") {
                if (toInsert > Number.MAX_SAFE_INTEGER) {
                    fatal(`Number too big: ${toInsert}`);
                }
            }

            row.json = toInsert;
            insertStmt.run(Object.values(row));
        }
    });

    // Increase the timeout for the transaction (in milliseconds)
    destDb.pragma("busy_timeout = 60000"); // 60 seconds

    // Execute the transaction
    insertTransaction();
}

function checkIntegrity(srcDb, destDb, tables) {
    for (const tableName of tables) {
        console.log(`Checking integrity for table: ${tableName}`);

        // Prepare SELECT statements
        const srcSelectStmt = srcDb.prepare(`SELECT * FROM ${tableName}`);
        const destSelectStmt = destDb.prepare(
            `SELECT * FROM ${tableName} WHERE ID = ?`
        );

        // Iterate over all rows in the source table
        for (const srcRow of srcSelectStmt.iterate()) {
            // Fetch the corresponding row from the destination table using the primary key (assuming 'id' as primary key)
            const destRow = destSelectStmt.get(srcRow.ID);

            // Check if the row exists in the destination table and is not null
            if (destRow && destRow !== null) {
                console.log(`Row with ID ${srcRow.ID} exists in both tables.`);
            } else {
                fatal(
                    `Row with ID ${srcRow.ID} is missing or null in the destination table.`
                );
            }
        }
    }
}

function main() {
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

    if (fs.existsSync(vargs.output)) {
        fatal(`output file already exist: ${vargs.output}`);
    }

    console.log(`Loading ${vargs.input} file`);
    const db = new Database(vargs.input);
    console.log("Sqlite loaded");

    console.log(`Creating output sqlite file: ${vargs.output}`);
    const dbOut = new Database(vargs.output);

    console.log("Getting tables");
    const tables = getTables(db);
    console.log(`Tables found: [${tables.join(", ")}]`);

    for (const table of tables) {
        processTable(db, dbOut, table);
    }

    console.log("Done!");

    if (vargs["check-integrity"]) {
        checkIntegrity(db, dbOut, tables);
        console.log("Done!");
    }

    db.close();
    dbOut.close();
}

main();
