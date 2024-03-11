import type { Stream, Writer } from "@ajuvercr/js-runner";
import { SDS } from "@treecg/types";
import { Store, Parser, DataFactory } from "n3";
import { CREATE, UPDATE, DELETE } from "./SPARQLQueries";
import { Quad_Subject, Term } from "@rdfjs/types";
import { doSPARQLRequest, sanitizeQuads } from "./Utils";

const { quad, namedNode } = DataFactory;

export type ChangeSemantics = {
    changeTypePath: string;
    createValue: string;
    updateValue: string;
    deleteValue: string;
};

export type TransactionConfig = {
    transactionIdPath: string;
    transactionEndPath: string;
};

export type IngestConfig = {
    memberIsGraph: boolean;
    memberShapes?: string[];
    changeSemantics?: ChangeSemantics;
    targetNamedGraph?: string;
    transactionConfig?: TransactionConfig;
    graphStoreUrl?: string;
};

export type TransactionMember = {
    memberId: string,
    transactionId: string,
    store: Store
}

export async function sparqlIngest(
    memberStream: Stream<string>,
    config: IngestConfig,
    sparqlWriter?: Writer<string>
) {
    let transactionMembers: TransactionMember[] = [];

    memberStream.data(async rawQuads => {
        const quads = new Parser().parse(rawQuads);
        const store = new Store(quads);

        // Get member IRI form SDS description
        const memberIRI = store.getObjects(null, SDS.payload, null)[0];

        if (memberIRI) {
            // Remove SDS wrapper
            store.removeQuads(store.getQuads(null, SDS.stream, null, null));
            store.removeQuads(store.getQuads(null, SDS.payload, null, null));

            // Find if this member is part of a transaction
            if (config.transactionConfig) {
                const transactionId = store.getObjects(null, config.transactionConfig.transactionIdPath, null)[0];
                if (transactionId) {
                    // Remove transactionId property
                    store.removeQuad(quad(
                        <Quad_Subject>memberIRI,
                        namedNode(config.transactionConfig.transactionIdPath),
                        transactionId
                    ));
                    // See if this is a finishing, new or ongoing transaction
                    const isLastOfTransaction = store.getObjects(null, config.transactionConfig.transactionEndPath, null)[0];

                    if (isLastOfTransaction) {
                        console.log(`[sparqlIngest] Last member of ${transactionId.value} received!`);
                        // Check this transaction is correct
                        verifyTransaction(transactionMembers.map(ts => ts.store), config.transactionConfig.transactionIdPath, transactionId);
                        // Remove is-last-of-transaction flag
                        store.removeQuad(quad(
                            <Quad_Subject>memberIRI,
                            namedNode(config.transactionConfig.transactionEndPath),
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
                        verifyTransaction(transactionMembers.map(ts => ts.store), config.transactionConfig.transactionIdPath, transactionId);
                        // Is an ongoing transaction, so we add this member's quads into the transaction store
                        transactionMembers.push({
                            memberId: memberIRI.value,
                            transactionId: transactionId.value,
                            store
                        });
                        return;
                    } else {
                        console.log(`[sparqlIngest] New transaction ${transactionId.value} started!`);
                        if (transactionMembers.length > 0)
                            throw new Error(`[sparqlIngest] Received new transaction ${transactionId.value}, `
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
            let queryType;

            if (config.changeSemantics) {
                if (transactionMembers.length > 0) {
                    query = createTransactionQueries(transactionMembers, config);
                    // Clean up transaction stores
                    transactionMembers = [];
                } else {
                    // Determine if we have a named graph (either explicitly configure or as the member itself)
                    const ng = getNamedGraphIfAny(memberIRI, config.memberIsGraph, config.targetNamedGraph);
                    // Get the type of change
                    const ctv = store.getQuads(null, config.changeSemantics!.changeTypePath, null, null)[0];
                    // Remove change type quad from store
                    store.removeQuad(ctv);
                    // Sanitize quads to prevent issues on SPARQL queries
                    sanitizeQuads(store);
                    // Assemble corresponding SPARQL UPDATE query
                    if (ctv.object.value === config.changeSemantics.createValue) {
                        query = CREATE(store, ng);
                        queryType = "CREATE";
                    } else if (ctv.object.value === config.changeSemantics.updateValue) {
                        query = UPDATE(store, ng);
                        queryType = "UPDATE";
                    } else if (ctv.object.value === config.changeSemantics.deleteValue) {
                        query = DELETE(store, [memberIRI.value], config.memberShapes, ng);
                        queryType = "DELETE";
                    } else {
                        throw new Error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
                    }
                }
            } else {
                if (transactionMembers.length > 0) {
                    transactionMembers.forEach(ts => store.addQuads(ts.store.getQuads(null, null, null, null)));
                    query = UPDATE(store, config.targetNamedGraph);
                } else {
                    // Determine if we have a named graph (either explicitly configure or as the member itself)
                    const ng = getNamedGraphIfAny(memberIRI, config.memberIsGraph, config.targetNamedGraph);
                    // No change semantics are provided so we do a DELETE/INSERT query by default
                    query = UPDATE(store, ng);
                }
            }

            // Execute the update query
            if (query) {
                const outputPromises = [];
                if (sparqlWriter) {
                    outputPromises.push(sparqlWriter.push(query));
                }
                if (config.graphStoreUrl) {
                    outputPromises.push(doSPARQLRequest(query, config.graphStoreUrl));
                }

                await Promise.all(outputPromises);
                console.log(`[sparqlIngest] Executed ${queryType} on remote SPARQL server ${config.graphStoreUrl} - ${new Date().toISOString()}`);
            } 
        } else {
            throw new Error(`[sparqlIngest] No member IRI found in received RDF data: \n${rawQuads}`);
        }
    });

    if (sparqlWriter) {
        memberStream.on("end", async () => await sparqlWriter.end());
    }
}

function verifyTransaction(stores: Store[], transactionIdPath: string, transactionId: Term): void {
    for (const store of stores) {
        // Get all transaction IDs
        const tIds = store.getObjects(null, transactionIdPath, null);
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
    console.log(`[sparqlIngest] Creating multi-operation SPARQL UPDATE query for ${transactionMembers.length}`
        + ` members of transaction ${transactionMembers[0].transactionId}`);
    // This is a transaction query, we need to deal with possibly multiple types of queries
    const createStore = new Store();
    const updateStore = new Store();
    const deleteStore = new Store();
    const deleteMembers: string[] = [];

    const transactionQueryBuilder: string[] = [];

    for (const tsm of transactionMembers) {
        const ctv = tsm.store.getQuads(null, config.changeSemantics!.changeTypePath, null, null)[0];
        // Remove change type quad from store
        tsm.store.removeQuad(ctv);

        if (ctv.object.value === config.changeSemantics!.createValue) {
            createStore.addQuads(tsm.store.getQuads(null, null, null, null));
        } else if (ctv.object.value === config.changeSemantics!.updateValue) {
            updateStore.addQuads(tsm.store.getQuads(null, null, null, null));
        } else if (ctv.object.value === config.changeSemantics!.deleteValue) {
            deleteStore.addQuads(tsm.store.getQuads(null, null, null, null));
            deleteMembers.push(tsm.memberId);
        } else {
            throw new Error(`[sparqlIngest] Unrecognized change type value: ${ctv.object.value}`);
        }
    }

    // TODO: Handle case of members as Named Graphs

    // Build multi-operation SPARQL query
    if (createStore.size > 0) {
        transactionQueryBuilder.push(CREATE(createStore, config.targetNamedGraph));
    }
    if (updateStore.size > 0) {
        transactionQueryBuilder.push(UPDATE(updateStore, config.targetNamedGraph));
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