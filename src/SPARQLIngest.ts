import { extendLogger, Processor, Reader, Writer } from "@rdfc/js-runner";
import { SDS } from "@treecg/types";
import { DataFactory } from "rdf-data-factory";
import { RdfStore } from "rdf-stores";
import { Parser } from "n3";
import { writeFile } from "fs/promises";
import { CREATE, DELETE, UPDATE } from "./SPARQLQueries";
import { doSPARQLRequest, getObjects, sanitizeQuads } from "./Utils";

import type { Quad_Subject, Term } from "@rdfjs/types";
import { Logger } from "winston";

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

export type IngestConfig = {
   memberIsGraph?: boolean;
   memberShapes?: string[]; // TODO: This should be obtained from an SDS metadata stream
   changeSemantics?: ChangeSemantics;
   targetNamedGraph?: string;
   transactionConfig?: TransactionConfig;
   graphStoreUrl?: string;
   forVirtuoso?: boolean; // For handling the hardcoded query limitations of Virtuoso
   accessToken?: string; // For SPARQL endpoints that require authentication like Qlever
   measurePerformance?: PerformanceConfig;
};

export type TransactionMember = {
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
   protected transactionMembers: TransactionMember[] = [];
   protected requestsPerformance: number[] = [];

   protected createTransactionQueriesLogger: Logger;
   protected doSPARQLRequestLogger: Logger;

   async init(this: SPARQLIngestArgs & this): Promise<void> {
      this.createTransactionQueriesLogger = extendLogger(this.logger, "createTransactionQueries");
      this.doSPARQLRequestLogger = extendLogger(this.logger, "doSPARQLRequest");
   }

   async transform(this: SPARQLIngestArgs & this): Promise<void> {
      for await (const rawQuads of this.memberStream.strings()) {
         this.logger.debug(`Raw member data received: \n${rawQuads}`);
         const quads = new Parser().parse(rawQuads);
         this.logger.verbose(`Parsed ${quads.length} quads from received member data`);
         const store = RdfStore.createDefault();
         quads.forEach(q => store.addQuad(q));

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

            // Find if this member is part of a transaction
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
                  // Determine if we have a named graph (either explicitly configured or as the member itself)
                  const ng = this.getNamedGraphIfAny(memberIRI, this.config.memberIsGraph, this.config.targetNamedGraph);
                  // Get the type of change
                  // TODO: use rdf-lens to support complex paths
                  const ctv = store.getQuads(
                     null,
                     df.namedNode(this.config.changeSemantics!.changeTypePath)
                  )[0];
                  // Remove change type quad from store
                  // TODO: this should be made configurable as not always we want to remove this quad.
                  store.removeQuad(ctv);
                  // Sanitize quads to prevent issues on SPARQL queries
                  sanitizeQuads(store);
                  // Assemble corresponding SPARQL UPDATE query
                  if (ctv.object.value === this.config.changeSemantics.createValue) {
                     this.logger.info(`Preparing 'INSERT DATA {}' SPARQL query for member ${memberIRI.value}`);
                     query = CREATE(store, this.config.forVirtuoso, ng);
                  } else if (ctv.object.value === this.config.changeSemantics.updateValue) {
                     this.logger.info(`Preparing 'DELETE {} INSERT {} WHERE {}' SPARQL query for member ${memberIRI.value}`);
                     query = UPDATE(store, this.config.forVirtuoso, ng);
                  } else if (ctv.object.value === this.config.changeSemantics.deleteValue) {
                     this.logger.info(`Preparing 'DELETE WHERE {}' SPARQL query for member ${memberIRI.value}`);
                     query = [DELETE(store, [memberIRI.value], this.config.memberShapes, ng)];
                  } else {
                     this.logger.error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
                     throw new Error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
                  }
               }
            } else {
               if (this.transactionMembers.length > 0) {
                  this.transactionMembers.forEach(ts => {
                     ts.store.getQuads(null, null, null, null).forEach(q => store.addQuad(q));
                  });
                  this.logger.info(`Preparing 'DELETE {} WHERE {} + INSERT DATA {}' SPARQL query for transaction member ${memberIRI.value}`);
                  query = UPDATE(store, this.config.forVirtuoso, this.config.targetNamedGraph);
               } else {
                  // Determine if we have a named graph (either explicitly configure or as the member itself)
                  const ng = this.getNamedGraphIfAny(memberIRI, this.config.memberIsGraph, this.config.targetNamedGraph);
                  // No change semantics are provided so we do a DELETE/INSERT query by default
                  this.logger.info(`Preparing 'DELETE {} WHERE {} + INSERT DATA {}' SPARQL query for member ${memberIRI.value}`);
                  query = UPDATE(store, this.config.forVirtuoso, ng);
               }
            }
         } else {
            // Non-SDS data

            // TODO: Handle change semantics(?) and transactions for non-SDS data
            this.logger.info(`Preparing 'DELETE {} WHERE {} + INSERT DATA {}' SPARQL query for received triples (${store.size})`);
            query = UPDATE(store, this.config.forVirtuoso, this.config.targetNamedGraph);
         }

         // Execute the update query
         if (query && query.length > 0) {
            this.logger.debug(`Complete SPARQL query generated for received member: \n${query.join("\n")}`);
            if (this.config.graphStoreUrl) {
               try {
                  const t0 = Date.now();
                  await doSPARQLRequest(query, this.config, this.doSPARQLRequestLogger);
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
               await this.sparqlWriter.string(query.join("\n"));
            }
         } else {
            this.logger.warn(`No query generated for member ${memberIRI.value}`);
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

   getNamedGraphIfAny(
      memberIRI: Term,
      memberIsGraph: boolean | undefined,
      targetNamedGraph?: string
   ): string | undefined {
      let ng;
      if (memberIsGraph) {
         ng = memberIRI.value;
      } else if (targetNamedGraph) {
         ng = targetNamedGraph;
      }
      return ng;
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
         // Remove change type quad from store
         tsm.store.removeQuad(ctv);

         if (ctv.object.value === config.changeSemantics!.createValue) {
            tsm.store.getQuads(null, null, null, null).forEach(q => createStore.addQuad(q));
         } else if (ctv.object.value === config.changeSemantics!.updateValue) {
            tsm.store.getQuads(null, null, null, null).forEach(q => updateStore.addQuad(q));
         } else if (ctv.object.value === config.changeSemantics!.deleteValue) {
            tsm.store.getQuads(null, null, null, null).forEach(q => deleteStore.addQuad(q));
            deleteMembers.push(tsm.memberId);
         } else {
            this.createTransactionQueriesLogger.error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
            throw new Error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
         }
      }

      // TODO: Handle case of members as Named Graphs

      // Build multi-operation SPARQL query
      if (createStore.size > 0) {
         transactionQueryBuilder.push(CREATE(createStore, config.forVirtuoso, config.targetNamedGraph).join("\n"));
      }
      if (updateStore.size > 0) {
         transactionQueryBuilder.push(UPDATE(updateStore, config.forVirtuoso, config.targetNamedGraph).join("\n"));
      }
      if (updateStore.size > 0) {
         transactionQueryBuilder.push(DELETE(
            deleteStore,
            deleteMembers,
            config.memberShapes,
            config.targetNamedGraph
         ));
      }

      return transactionQueryBuilder.join(";\n");
   }
}
