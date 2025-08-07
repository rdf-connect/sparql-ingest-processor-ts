import type { Stream, Writer } from "@rdfc/js-runner";
import { SDS } from "@treecg/types";
import { DataFactory } from "rdf-data-factory";
import { RdfStore } from "rdf-stores";
import { Parser } from "n3";
import { CREATE, UPDATE, DELETE } from "./SPARQLQueries";
import {
    doSPARQLRequest,
    sanitizeQuads,
    getObjects
} from "./Utils";
import { getLoggerFor } from "./LogUtil";

import type { Quad_Subject, Term } from "@rdfjs/types";

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

export type IngestConfig = {
    memberIsGraph: boolean;
    maxQueryLength: number;
    memberShapes?: string[]; // TODO: This should be obtained from an SDS metadata stream
    changeSemantics?: ChangeSemantics;
    targetNamedGraph?: string;
    transactionConfig?: TransactionConfig;
    graphStoreUrl?: string;
    accessToken?: string; // For SPARQL endpoints that require authentication like Qlever
    measurePerformance?: boolean; // If true, performance metrics will be logged
};

export type TransactionMember = {
    memberId: string,
    transactionId: string,
    store: RdfStore
}

export async function sparqlIngest(
    memberStream: Stream<string>,
    config: IngestConfig,
    sparqlWriter?: Writer<string>
) {
    const logger = getLoggerFor("sparqlIngest");
    let transactionMembers: TransactionMember[] = [];
    const requestsPerformance: number[] = [];

    memberStream.data(async rawQuads => {
        logger.debug(`Raw member data received: \n${rawQuads}`);
        const quads = new Parser().parse(rawQuads);
        logger.verbose(`Parsed ${quads.length} quads from received member data`);
        const store = RdfStore.createDefault();
        quads.forEach(q => store.addQuad(q));

        // Get member IRI form SDS description
        const memberIRI = getObjects(store, null, SDS.terms.payload, SDS.terms.custom("DataDescription"))[0];
        logger.verbose(`Member IRI found: ${memberIRI ? memberIRI.value : "none"}`);
        // TODO: produce some SDS metadata about the processing taking place here

        if (memberIRI) {
            // Remove SDS wrapper quads
            const sdsQuads = store.getQuads(null, null, null, SDS.terms.custom("DataDescription"));
            sdsQuads.forEach(q => store.removeQuad(q));

            // Find if this member is part of a transaction
            if (config.transactionConfig) {
                // TODO: use rdf-lens to support complex paths
                const transactionId = getObjects(
                    store,
                    null,
                    df.namedNode(config.transactionConfig.transactionIdPath),
                    null
                )[0];
                if (transactionId) {
                    // Remove transactionId property
                    store.removeQuad(df.quad(
                        <Quad_Subject>memberIRI,
                        df.namedNode(config.transactionConfig.transactionIdPath),
                        transactionId
                    ));
                    // See if this is a finishing, new or ongoing transaction
                    // TODO: use rdf-lens to support complex paths
                    const isLastOfTransaction = getObjects(
                        store,
                        null,
                        df.namedNode(config.transactionConfig.transactionEndPath),
                        null
                    )[0];

                    if (isLastOfTransaction) {
                        logger.info(`Last member of ${transactionId.value} received!`);
                        // Check this transaction is correct
                        verifyTransaction(
                            transactionMembers.map(ts => ts.store),
                            config.transactionConfig.transactionIdPath,
                            transactionId
                        );
                        // Remove is-last-of-transaction flag
                        // This might not be needed
                        store.removeQuad(df.quad(
                            <Quad_Subject>memberIRI,
                            df.namedNode(config.transactionConfig.transactionEndPath),
                            isLastOfTransaction
                        ));
                        // We copy all previous member quads into the current store
                        transactionMembers.push({
                            memberId: memberIRI.value,
                            transactionId: transactionId.value,
                            store
                        });
                    } else if (transactionMembers.length > 0) {
                        // Check this transaction is correct
                        verifyTransaction(
                            transactionMembers.map(ts => ts.store),
                            config.transactionConfig.transactionIdPath,
                            transactionId
                        );
                        // Is an ongoing transaction, so we add this member's quads into the transaction store
                        transactionMembers.push({
                            memberId: memberIRI.value,
                            transactionId: transactionId.value,
                            store
                        });
                        return;
                    } else {
                        logger.info(`New transaction ${transactionId.value} started!`);
                        if (transactionMembers.length > 0)
                            throw new Error(`Received new transaction ${transactionId.value}, `
                                + `but older transaction ${transactionMembers[0].transactionId} hasn't been finalized `);
                        // Is a new transaction, add it to the transaction store
                        transactionMembers.push({
                            memberId: memberIRI.value,
                            transactionId: transactionId.value,
                            store
                        });
                        return;
                    }
                }
            }

            // Variable that will hold the full query to be executed
            let query;

            if (config.changeSemantics) {
                if (transactionMembers.length > 0) {
                    query = createTransactionQueries(transactionMembers, config);
                    // Clean up transaction stores
                    transactionMembers = [];
                } else {
                    // Determine if we have a named graph (either explicitly configured or as the member itself)
                    const ng = getNamedGraphIfAny(memberIRI, config.memberIsGraph, config.targetNamedGraph);
                    // Get the type of change
                    // TODO: use rdf-lens to support complex paths
                    const ctv = store.getQuads(
                        null,
                        df.namedNode(config.changeSemantics!.changeTypePath)
                    )[0];
                    // Remove change type quad from store
                    // TODO: this should be made configurable as not always we want to remove this quad.
                    store.removeQuad(ctv);
                    // Sanitize quads to prevent issues on SPARQL queries
                    sanitizeQuads(store);
                    // Assemble corresponding SPARQL UPDATE query
                    if (ctv.object.value === config.changeSemantics.createValue) {
                        logger.info(`Preparing 'INSERT DATA {}' SPARQL query for member ${memberIRI.value}`);
                        query = CREATE(store, config.maxQueryLength, ng);
                    } else if (ctv.object.value === config.changeSemantics.updateValue) {
                        logger.info(`Preparing 'DELETE {} INSERT {} WHERE {}' SPARQL query for member ${memberIRI.value}`);
                        query = UPDATE(store, config.maxQueryLength, ng);
                    } else if (ctv.object.value === config.changeSemantics.deleteValue) {
                        logger.info(`Preparing 'DELETE WHERE {}' SPARQL query for member ${memberIRI.value}`);
                        query = DELETE(store, [memberIRI.value], config.memberShapes, ng);
                    } else {
                        throw new Error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
                    }
                }
            } else {
                if (transactionMembers.length > 0) {
                    transactionMembers.forEach(ts => {
                        ts.store.getQuads(null, null, null, null).forEach(q => store.addQuad(q));
                    });
                    logger.info(`Preparing 'DELETE {} WHERE {} + INSERT DATA {}' SPARQL query for transaction member ${memberIRI.value}`);
                    query = UPDATE(store, config.maxQueryLength, config.targetNamedGraph);
                } else {
                    // Determine if we have a named graph (either explicitly configure or as the member itself)
                    const ng = getNamedGraphIfAny(memberIRI, config.memberIsGraph, config.targetNamedGraph);
                    // No change semantics are provided so we do a DELETE/INSERT query by default
                    logger.info(`Preparing 'DELETE {} WHERE {} + INSERT DATA {}' SPARQL query for member ${memberIRI.value}`);
                    query = UPDATE(store, config.maxQueryLength, ng);
                }
            }

            // Execute the update query
            if (query) {
                logger.debug(`Generated SPARQL query: \n${query}`);
                if (config.graphStoreUrl) {
                    const t0 = Date.now();
                    await doSPARQLRequest(query, config);
                    const reqTime = Date.now() - t0;
                    if (config.measurePerformance) {
                        requestsPerformance.push(reqTime);
                    }
                    logger.info(`Executed query on remote SPARQL server ${config.graphStoreUrl} (took ${reqTime} ms)`);
                }

                if (sparqlWriter) {
                    await sparqlWriter.push(query);
                }
            } else {
                logger.warn(`No query generated for member ${memberIRI.value}`);
            }
        } else {
            throw new Error(`[sparqlIngest] No member IRI found in received RDF data: \n${rawQuads}`);
        }
    });

    memberStream.on("end", async () => {
        if (sparqlWriter) {
            await sparqlWriter.end();
        }
        if (config.measurePerformance) {
            console.log(requestsPerformance);
        }
    });
}

function verifyTransaction(stores: RdfStore[], transactionIdPath: string, transactionId: Term): void {
    for (const store of stores) {
        // Get all transaction IDs
        const tIds = getObjects(store, null, df.namedNode(transactionIdPath), null);
        for (const tid of tIds) {
            if (!tid.equals(transactionId)) {
                throw new Error(`[sparqlIngest] Received non-matching transaction ID ${transactionId.value} `
                    + `with previous transaction: ${tid.value}`);
            }
        }
    }
}

function getNamedGraphIfAny(
    memberIRI: Term,
    memberIsGraph: boolean,
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

function createTransactionQueries(
    transactionMembers: TransactionMember[],
    config: IngestConfig,

): string {
    const logger = getLoggerFor("createTransactionQueries");
    logger.info(`Creating multi-operation SPARQL UPDATE query for ${transactionMembers.length}`
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
            throw new Error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
        }
    }

    // TODO: Handle case of members as Named Graphs

    // Build multi-operation SPARQL query
    if (createStore.size > 0) {
        transactionQueryBuilder.push(CREATE(createStore, config.maxQueryLength, config.targetNamedGraph));
    }
    if (updateStore.size > 0) {
        transactionQueryBuilder.push(UPDATE(updateStore, config.maxQueryLength, config.targetNamedGraph));
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