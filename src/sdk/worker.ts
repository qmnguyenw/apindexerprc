import { aptos } from "@aptos-labs/aptos-protos";
import { Config } from "./config";
import { Timer } from "timer-node";
import { exit } from "process";
import { ChannelCredentials, Metadata, StatusObject } from "@grpc/grpc-js";
import { parse as pgConnParse } from "pg-connection-string";
import { createDataSource } from "./models/data_source";
import {
  NextVersionToProcess,
  createNextVersionToProcess,
} from "./models/next_version_to_process";
import { Base } from "./models/base";
import { TransactionsProcessor } from "./processor";
import { DataSource } from "typeorm";
import { readFileSync, writeFileSync } from "fs";
import { join } from "path";

/**
 * This worker class is responsible for connecting to the txn stream and dispatching
 * transactions to the provided processor. It is responsible for tracking which
 * version it has processed up to and checkpointing that version in the database.
 */
export class Worker {
  // Base configuration required to connect to the database and the data stream.
  config: Config;

  // The processor we'll run.
  processor: TransactionsProcessor;

  // The DB connection.
  dataSource: DataSource;

  constructor({
    config,
    processor,
    models,
  }: {
    config: Config;
    processor: TransactionsProcessor;
    // Additional models for which we want to create tables in the DB.
    models: (typeof Base)[];
  }) {
    const options = pgConnParse(config.db_connection_uri);
    const port = options.port || "5432";

    if (!options.host || !options.database) {
      throw new Error(
        "Invalid postgres connection string. e.g. postgres://someuser:somepassword@somehost:5432/somedatabase",
      );
    }

    // Build the DB connection object.
    const dataSource = createDataSource({
      host: options.host!,
      port: Number(port),
      username: options.user,
      password: options.password,
      database: options.database,
      enableSSL: options.ssl as boolean,
      additionalEntities: models,
    });

    this.config = config;
    this.processor = processor;
    this.dataSource = dataSource;
  }

  /**
   * This function is the main entry point for the processor. It will connect to the txn
   * stream and start processing transactions. It will call the provided parse function to
   * parse transactions and insert the parsed data into the database.
   */
  async run({ perf }: { perf?: number } = {}) {
    await this.dataSource.initialize();
    const currentTxnVersion = BigInt(this.config.starting_version || 0n);
    let next_ver = await this.dataSource
      .getRepository(NextVersionToProcess)
      .find();
    if (next_ver && next_ver.length > 0) {
      console.log(next_ver[0].nextVersion);
      await this.startTransactionStream(BigInt(next_ver[0].nextVersion), perf);
    } else {
      await this.startTransactionStream(currentTxnVersion, perf);
    }
  }

  async startTransactionStream(startingVersion: bigint, perf?: number) {
    // Create the grpc client.
    const client = new aptos.indexer.v1.RawDataClient(
      this.config.grpc_data_stream_endpoint,
      ChannelCredentials.createSsl(),
      {
        "grpc.keepalive_time_ms": 1000,
        // 0 - No compression
        // 1 - Compress with DEFLATE algorithm
        // 2 - Compress with GZIP algorithm
        // 3 - Stream compression with GZIP algorithm
        "grpc.default_compression_algorithm": 2,
        // 0 - No compression
        // 1 - Low compression level
        // 2 - Medium compression level
        // 3 - High compression level
        "grpc.default_compression_level": 3,
        // -1 means unlimited
        "grpc.max_receive_message_length": -1,
        // -1 means unlimited
        "grpc.max_send_message_length": -1,
      },
    );

    let currentTxnVersion = startingVersion;

    console.log(
      `[Parser] Requesting stream starting from version ${startingVersion}`,
    );

    const request: aptos.indexer.v1.GetTransactionsRequest = {
      startingVersion,
    };

    const metadata = new Metadata();
    metadata.set(
      "Authorization",
      `Bearer ${this.config.grpc_data_stream_api_key}`,
    );
    const stream = client.getTransactions(request, metadata);

    const timer = new Timer();
    timer.start();

    stream.on(
      "data",
      async (response: aptos.indexer.v1.TransactionsResponse) => {
        console.log("hi");
        stream.pause();
        const transactions = response.transactions;

        if (transactions == null) {
          return;
        }

        // Validate response chain ID matches expected chain ID
        if (response.chainId != this.config.chain_id) {
          throw new Error(
            `Chain ID mismatch. Expected ${this.config.chain_id} but got ${response.chainId}`,
          );
        }

        const startVersion = transactions[0].version!;
        const endVersion = transactions[transactions.length - 1].version!;

        console.log({
          message: "[Parser] Response received",
          startVersion: startVersion,
        });

        if (startVersion != currentTxnVersion) {
          throw new Error(
            `Transaction version mismatch. Expected ${currentTxnVersion} but got ${startVersion}`,
          );
        }

        // this.syncWriteFile("response.json", JSON.stringify(aptos.indexer.v1.TransactionsResponse.toJSON(response)));

        // Pass the transactions to the given TransactionProcessor. It is responsible
        // for doing its own DB mutations.
        const processingResult = await this.processor.processTransactions({
          transactions,
          startVersion,
          endVersion,
          dataSource: this.dataSource,
        });

        const numProcessed =
          processingResult.endVersion - processingResult.startVersion;

        currentTxnVersion = endVersion;

        if (numProcessed) {
          await this.dataSource.transaction(async (txnManager) => {
            console.log("numProcessed");
            const nextVersionToProcess = createNextVersionToProcess({
              indexerName: this.processor.name(),
              version: currentTxnVersion + 1n,
            });
            await txnManager.upsert(
              NextVersionToProcess,
              nextVersionToProcess,
              ["indexerName"],
            );
          });
        } else if (currentTxnVersion % 1000n === 0n) {
          // Checkpoint
          console.log("Checkpoint");
          const nextVersionToProcess = createNextVersionToProcess({
            indexerName: this.processor.name(),
            version: currentTxnVersion + 1n,
          });
          await this.dataSource
            .getRepository(NextVersionToProcess)
            .upsert(nextVersionToProcess, ["indexerName"]);

          console.log({
            message: "[Parser] Successfully processed transactions",
            last_success_transaction_version: currentTxnVersion,
          });
        }

        const totalTransactions = currentTxnVersion - startingVersion + 1n;

        if (perf && totalTransactions >= Number(perf)) {
          timer.stop();
          console.log(
            `[Parser] It took ${timer.ms()} ms to process ${totalTransactions} txns`,
          );
          exit(0);
        }

        currentTxnVersion += 1n;
        stream.resume();
      },
    );

    stream.on("error", async (e) => {
      console.error(e);
      this.syncWriteFile("error.txt", e.message);
      if (
        e.message.includes(
          "8 RESOURCE_EXHAUSTED: Bandwidth exhausted or memory limit exceeded",
        )
      ) {
        const nextVersionToProcess = createNextVersionToProcess({
          indexerName: this.processor.name(),
          version: currentTxnVersion + 1000n,
        });
        let next_ver = await this.dataSource
          .getRepository(NextVersionToProcess)
          .find();
        console.log(next_ver[0].nextVersion);
        await this.dataSource
          .getRepository(NextVersionToProcess)
          .upsert(nextVersionToProcess, ["indexerName"]);
        await this.startTransactionStream(
          BigInt(next_ver[0].nextVersion),
          perf,
        );
      } else {
        let next_ver = await this.dataSource
          .getRepository(NextVersionToProcess)
          .find();
        console.log(next_ver[0].nextVersion);
        await this.startTransactionStream(
          BigInt(next_ver[0].nextVersion),
          perf,
        );
      }
    });

    stream.on("status", function (status) {
      console.log(`[Parser] ${status}`);
      // process status
    });
  }

  // write to file SYNCHRONOUSLY
  syncWriteFile(filename: string, data: any) {
    /**
     * flags:
     *  - w = Open file for reading and writing. File is created if not exists
     *  - a+ = Open file for reading and appending. The file is created if not exists
     */
    writeFileSync(join(__dirname, filename), data, {
      flag: "w",
    });

    const contents = readFileSync(join(__dirname, filename), "utf-8");
    // console.log(contents);
    // console.log(__dirname + filename);
    return contents;
  }
}