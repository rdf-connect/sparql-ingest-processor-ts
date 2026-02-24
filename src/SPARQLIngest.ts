import { Processor, extendLogger } from "@rdfc/js-runner";
import { SDS } from "@treecg/types";
import { Agent } from "undici";
import { DataFactory } from "rdf-data-factory";
import { RdfStore } from "rdf-stores";
import { Parser } from "n3";
import { writeFile } from "fs/promises";
import { CREATE, DELETE, UPDATE } from "./SPARQLQueries";
import { doSPARQLRequest, getObjects, sanitizeQuads } from "./Utils";
import { Logger } from "winston";

import type {
   Quad,
   Quad_Graph,
   Quad_Object,
   Quad_Predicate,
   Quad_Subject,
   Term
} from "@rdfjs/types";
import type { Reader, Writer } from "@rdfc/js-runner";

const df = new DataFactory();

// TODO: This should be obtained from an SDS metadata stream
export type ChangeSemantics = {
   changeTypePath: string;
   createValue: string;
   updateValue: string;
   deleteValue: string;
};

// TODO: This should be obtained from an SDS metadata stream
export type TransactionConfig = {
   transactionIdPath: string;
   transactionEndPath: string;
};

export type PerformanceConfig = {
   name: string;
   outputPath: string;
   queryTimeout?: number; // Timeout for SPARQL queries in seconds
   failureIsFatal?: boolean;
};

export enum OperationMode {
   REPLICATION = "Replication",
   SYNC = "Sync"
}

export type IngestConfig = {
   operationMode?: OperationMode;
   memberBatchSize?: number;
   memberShape?: string; // TODO: This should be obtained from an SDS metadata stream
   changeSemantics?: ChangeSemantics;
   targetNamedGraph?: string;
   transactionConfig?: TransactionConfig;
   graphStoreUrl?: string;
   forVirtuoso?: boolean; // For handling the hardcoded query limitations of Virtuoso
   accessToken?: string; // For SPARQL endpoints that require authentication like Qlever
   measurePerformance?: PerformanceConfig;
};

type TransactionMember = {
   memberId: string,
   transactionId: string,
   store: RdfStore
}

type SPARQLIngestArgs = {
   memberStream: Reader;
   config: IngestConfig;
   sparqlWriter?: Writer;
}

export class SPARQLIngest extends Processor<SPARQLIngestArgs> {
   protected globalDispatcher: Agent;
   protected transactionMembers: TransactionMember[] = [];
   protected memberBatch: Quad[] = [];
   protected requestsPerformance: number[] = [];
   protected batchCount = 0;

   protected createTransactionQueriesLogger: Logger;
   protected doSPARQLRequestLogger: Logger;

   async init(this: SPARQLIngestArgs & this): Promise<void> {
      this.createTransactionQueriesLogger = extendLogger(this.logger, "createTransactionQueries");
      this.doSPARQLRequestLogger = extendLogger(this.logger, "doSPARQLRequest");

      // HTTP requests timeouts (defaults to 10 minutes if not specified)
      this.globalDispatcher = new Agent({
         headersTimeout: (this.config.measurePerformance?.queryTimeout || 600) * 1000,
         bodyTimeout: (this.config.measurePerformance?.queryTimeout || 600) * 1000,
      });

      if (!this.config.operationMode) {
         this.config.operationMode = OperationMode.SYNC;
      }

      if (!this.config.memberBatchSize) {
         this.config.memberBatchSize = 100;
      }

      if (this.config.accessToken === "") {
         this.config.accessToken = undefined;
      }
   }

   async transform(this: SPARQLIngestArgs & this): Promise<void> {

      for await (const rawQuads of this.memberStream.strings()) {
         this.logger.debug(`Raw member data received: \n${rawQuads}`);
         const quads = new Parser().parse(rawQuads);
         this.logger.verbose(`Parsed ${quads.length} quads from received member data`);
         const store = RdfStore.createDefault();

         // Assign default graph triples to the target named graph (if any)
         quads.forEach(q => {
            if (q.graph.equals(df.defaultGraph()) && this.config.targetNamedGraph) {
               store.addQuad(df.quad(
                  <Quad_Subject>q.subject,
                  <Quad_Predicate>q.predicate,
                  <Quad_Object>q.object,
                  <Quad_Graph>df.namedNode(this.config.targetNamedGraph)
               ));
            } else {
               store.addQuad(q)
            }
         });

         // Sanitize quads to prevent issues on SPARQL queries
         sanitizeQuads(store);

         // Variable that will hold the full query to be executed
         let query: string[] | undefined;

         // Get member IRI form SDS description (if any)
         const memberIRI = getObjects(store, null, SDS.terms.payload, SDS.terms.custom("DataDescription"))[0];

         if (memberIRI) {
            this.logger.verbose(`Member IRI found in SDS metadata: ${memberIRI.value}`);
            // TODO: produce some SDS metadata about the processing taking place here

            // Remove SDS wrapper quads
            const sdsQuads = store.getQuads(null, null, null, SDS.terms.custom("DataDescription"));
            sdsQuads.forEach(q => store.removeQuad(q));

            // See if this member is part of a transaction
            if (this.config.transactionConfig) {
               // TODO: use rdf-lens to support complex paths
               const transactionId = getObjects(
                  store,
                  null,
                  df.namedNode(this.config.transactionConfig.transactionIdPath),
                  null
               )[0];
               if (transactionId) {
                  // Remove transactionId property
                  store.removeQuad(df.quad(
                     <Quad_Subject>memberIRI,
                     df.namedNode(this.config.transactionConfig.transactionIdPath),
                     transactionId
                  ));
                  // See if this is a finishing, new or ongoing transaction
                  // TODO: use rdf-lens to support complex paths
                  const isLastOfTransaction = getObjects(
                     store,
                     null,
                     df.namedNode(this.config.transactionConfig.transactionEndPath),
                     null
                  )[0];

                  if (isLastOfTransaction) {
                     this.logger.info(`Last member of ${transactionId.value} received!`);
                     // Check this transaction is correct
                     this.verifyTransaction(
                        this.transactionMembers.map(ts => ts.store),
                        this.config.transactionConfig.transactionIdPath,
                        transactionId
                     );
                     // Remove is-last-of-transaction flag
                     // This might not be needed
                     store.removeQuad(df.quad(
                        <Quad_Subject>memberIRI,
                        df.namedNode(this.config.transactionConfig.transactionEndPath),
                        isLastOfTransaction
                     ));
                     // We copy all previous member quads into the current store
                     this.transactionMembers.push({
                        memberId: memberIRI.value,
                        transactionId: transactionId.value,
                        store
                     });
                  } else if (this.transactionMembers.length > 0) {
                     // Check this transaction is correct
                     this.verifyTransaction(
                        this.transactionMembers.map(ts => ts.store),
                        this.config.transactionConfig.transactionIdPath,
                        transactionId
                     );
                     // Is an ongoing transaction, so we add this member's quads into the transaction store
                     this.transactionMembers.push({
                        memberId: memberIRI.value,
                        transactionId: transactionId.value,
                        store
                     });
                     continue;
                  } else {
                     this.logger.info(`New transaction ${transactionId.value} started!`);
                     if (this.transactionMembers.length > 0) {
                        this.logger.error(`Received new transaction ${transactionId.value}, `
                           + `but older transaction ${this.transactionMembers[0].transactionId} hasn't been finalized `);
                        throw new Error(`Received new transaction ${transactionId.value}, `
                           + `but older transaction ${this.transactionMembers[0].transactionId} hasn't been finalized `);
                     }
                     // Is a new transaction, add it to the transaction store
                     this.transactionMembers.push({
                        memberId: memberIRI.value,
                        transactionId: transactionId.value,
                        store
                     });
                     continue;
                  }
               }
            }

            if (this.config.changeSemantics) {
               if (this.transactionMembers.length > 0) {
                  query = [this.createTransactionQueries(this.transactionMembers, this.config)];
                  // Clean up transaction stores
                  this.transactionMembers = [];
               } else {
                  // Get the type of change
                  // TODO: use rdf-lens to support complex paths
                  const ctv = store.getQuads(
                     null,
                     df.namedNode(this.config.changeSemantics!.changeTypePath)
                  )[0];
                  // Assemble corresponding SPARQL UPDATE query
                  if (ctv.object.value === this.config.changeSemantics.createValue) {
                     this.logger.info(`Preparing 'INSERT DATA {}' SPARQL query for member ${memberIRI.value}`);
                     query = CREATE(store, this.config.forVirtuoso);
                  } else if (ctv.object.value === this.config.changeSemantics.updateValue) {
                     this.logger.info(`Preparing 'DELETE {} INSERT {} WHERE {}' SPARQL query for member ${memberIRI.value}`);
                     query = UPDATE(store, this.config.forVirtuoso);
                  } else if (ctv.object.value === this.config.changeSemantics.deleteValue) {
                     this.logger.info(`Preparing 'DELETE WHERE {}' SPARQL query for member ${memberIRI.value}`);
                     query = DELETE(store, memberIRI.value, this.config.memberShape);
                  } else {
                     this.logger.error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
                     throw new Error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
                  }
               }
            } else {
               if (this.transactionMembers.length > 0) {
                  this.transactionMembers.forEach(ts => {
                     ts.store.getQuads().forEach(q => store.addQuad(q));
                  });
                  this.logger.info(`Preparing 'DELETE {} WHERE {} + INSERT DATA {}' SPARQL query for transaction member ${memberIRI.value}`);
                  query = UPDATE(store, this.config.forVirtuoso);
               } else {
                  // Check operation mode
                  if (this.config.operationMode === OperationMode.REPLICATION) {
                     this.memberBatch.push(...store.getQuads());
                     this.batchCount++;
                     if (this.batchCount < this.config.memberBatchSize!) {
                        continue;
                     }
                  } else {
                     // No change semantics are provided so we do a DELETE/INSERT query by default
                     this.logger.info(`Preparing 'DELETE {} WHERE {} + INSERT DATA {}' SPARQL queries for member ${memberIRI.value}`);
                     query = UPDATE(store, this.config.forVirtuoso);
                  }
               }
            }
         } else {
            // We got non-SDS data
            // TODO: Handle change semantics(?) and transactions(?) for non-SDS data

            // Check operation mode
            if (this.config.operationMode === OperationMode.REPLICATION) {
               // Build batch of quads that will be sent in one go using the SPARQL Graph Store protocol
               this.memberBatch.push(...store.getQuads());
               this.batchCount++;
               if (this.batchCount < this.config.memberBatchSize!) {
                  continue;
               }
            } else {
               this.logger.info(`Preparing 'DELETE {} WHERE {} + INSERT DATA {}' SPARQL queries for received quads (${store.size})`);
               query = UPDATE(store, this.config.forVirtuoso);
            }
         }

         // Execute the update query
         if (query && query.length > 0) {
            if (this.config.graphStoreUrl) {
               try {
                  const t0 = Date.now();
                  await doSPARQLRequest(
                     query,
                     this.config,
                     this.globalDispatcher,
                     this.logger
                  );
                  const reqTime = Date.now() - t0;
                  if (this.config.measurePerformance) {
                     this.requestsPerformance.push(reqTime);
                  }
                  this.logger.info(`Executed query on remote SPARQL server ${this.config.graphStoreUrl} (took ${reqTime} ms)`);
               } catch (error) {
                  if (!this.config.measurePerformance || this.config.measurePerformance.failureIsFatal) {
                     this.logger.error(`Error executing query on remote SPARQL server ${this.config.graphStoreUrl}: ${error}`);
                     throw error;
                  } else {
                     if (this.config.measurePerformance) {
                        this.requestsPerformance.push(-1); // -1 indicates a failure
                     }
                  }
               }
            }

            if (this.sparqlWriter) {
               if (this.config.forVirtuoso) {
                  for (const q of query) {
                     await this.sparqlWriter.string(q);
                  }
               } else {
                  await this.sparqlWriter.string(query.join(";\n"));
               }
            }
         } else {
            if (this.config.operationMode === OperationMode.REPLICATION) {
               try {
                  // Execute the ingestion of the collected member batch via the SPARQL Graph Store protocol
                  const t0 = Date.now();
                  await doSPARQLRequest(
                     this.memberBatch,
                     this.config,
                     this.globalDispatcher,
                     this.logger
                  );
                  const reqTime = Date.now() - t0;
                  if (this.config.measurePerformance) {
                     this.requestsPerformance.push(reqTime);
                  }
                  this.logger.info(`Executed query on remote SPARQL server ${this.config.graphStoreUrl} (took ${reqTime} ms)`);
                  this.batchCount = 0;
                  this.memberBatch = [];
               } catch (error) {
                  if (!this.config.measurePerformance || this.config.measurePerformance.failureIsFatal) {
                     this.logger.error(`Error executing query on remote SPARQL server ${this.config.graphStoreUrl}: ${error}`);
                     throw error;
                  } else {
                     if (this.config.measurePerformance) {
                        this.requestsPerformance.push(-1); // -1 indicates a failure
                     }
                  }
               }
            } else {
               this.logger.warn(`No query generated for member ${memberIRI.value}`);
            }
         }
      }

      // Flush remaining member batch if any
      if (this.config.operationMode === OperationMode.REPLICATION && this.memberBatch.length > 0) {
         try {
            // Execute the ingestion of the collected member batch via the SPARQL Graph Store protocol
            const t0 = Date.now();
            await doSPARQLRequest(
               this.memberBatch,
               this.config,
               this.globalDispatcher,
               this.logger
            );
            const reqTime = Date.now() - t0;
            if (this.config.measurePerformance) {
               this.requestsPerformance.push(reqTime);
            }
            this.logger.info(`Executed query on remote SPARQL server ${this.config.graphStoreUrl} (took ${reqTime} ms)`);
            this.batchCount = 0;
            this.memberBatch = [];
         } catch (error) {
            if (!this.config.measurePerformance || this.config.measurePerformance.failureIsFatal) {
               this.logger.error(`Error executing query on remote SPARQL server ${this.config.graphStoreUrl}: ${error}`);
               throw error;
            } else {
               if (this.config.measurePerformance) {
                  this.requestsPerformance.push(-1); // -1 indicates a failure
               }
            }
         }
      }

      if (this.sparqlWriter) {
         this.logger.info("Closing SPARQL writer");
         await this.sparqlWriter.close();
      }
      if (this.config.measurePerformance) {
         await writeFile(
            `${this.config.measurePerformance.outputPath}/${this.config.measurePerformance.name}.json`,
            JSON.stringify(this.requestsPerformance),
            "utf-8"
         );
      }

      // Gracefully close the global dispatcher
      await this.globalDispatcher.close();
   }

   async produce(this: SPARQLIngestArgs & this): Promise<void> {
      // Nothing to do here, everything is done in the member stream processing
   }

   verifyTransaction(stores: RdfStore[], transactionIdPath: string, transactionId: Term): void {
      for (const store of stores) {
         // Get all transaction IDs
         const tIds = getObjects(store, null, df.namedNode(transactionIdPath), null);
         for (const tid of tIds) {
            if (!tid.equals(transactionId)) {
               this.logger.error(`[sparqlIngest] Received non-matching transaction ID ${transactionId.value} `
                  + `with previous transaction: ${tid.value}`);
               throw new Error(`[sparqlIngest] Received non-matching transaction ID ${transactionId.value} `
                  + `with previous transaction: ${tid.value}`);
            }
         }
      }
   }

   createTransactionQueries(
      transactionMembers: TransactionMember[],
      config: IngestConfig,
   ): string {
      this.createTransactionQueriesLogger.info(`Creating multi-operation SPARQL UPDATE query for ${transactionMembers.length}`
         + ` members of transaction ${transactionMembers[0].transactionId}`);
      // This is a transaction query, we need to deal with possibly multiple types of queries
      const createStore = RdfStore.createDefault();
      const updateStore = RdfStore.createDefault();
      const deleteStore = RdfStore.createDefault();
      const deleteMembers: string[] = [];

      const transactionQueryBuilder: string[] = [];

      for (const tsm of transactionMembers) {
         const ctv = tsm.store.getQuads(null, df.namedNode(config.changeSemantics!.changeTypePath))[0];

         if (ctv.object.value === config.changeSemantics!.createValue) {
            tsm.store.getQuads().forEach(q => createStore.addQuad(q));
         } else if (ctv.object.value === config.changeSemantics!.updateValue) {
            tsm.store.getQuads().forEach(q => updateStore.addQuad(q));
         } else if (ctv.object.value === config.changeSemantics!.deleteValue) {
            tsm.store.getQuads().forEach(q => deleteStore.addQuad(q));
            deleteMembers.push(tsm.memberId);
         } else {
            this.createTransactionQueriesLogger.error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
            throw new Error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
         }
      }

      // Build multi-operation SPARQL query
      if (createStore.size > 0) {
         transactionQueryBuilder.push(...CREATE(createStore, config.forVirtuoso));
      }
      if (updateStore.size > 0) {
         transactionQueryBuilder.push(...UPDATE(updateStore, config.forVirtuoso));
      }
      if (deleteStore.size > 0) {
         deleteMembers.forEach(dm => {
            transactionQueryBuilder.push(...DELETE(
               deleteStore,
               dm,
               config.memberShape,
            ));
         });
      }
      return transactionQueryBuilder.join(";\n");
   }
}
