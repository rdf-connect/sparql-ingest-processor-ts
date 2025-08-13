import { XSD } from "@treecg/types";
import { DataFactory } from "rdf-data-factory";
import { RdfStore } from "rdf-stores";
import { getLoggerFor } from "./LogUtil";
import { Agent } from "undici";

import type { Term, Quad_Subject, Quad_Object } from "@rdfjs/types";
import type { IngestConfig } from "./SPARQLIngest";

const df = new DataFactory();

export function getSubjects(
    store: RdfStore,
    predicate: Term | null,
    object: Term | null,
    graph?: Term | null,
): Quad_Subject[] {
    return store.getQuads(null, predicate, object, graph).map((quad) => {
        return quad.subject;
    });
}

export function getObjects(
    store: RdfStore,
    subject: Term | null,
    predicate: Term | null,
    graph?: Term | null,
): Quad_Object[] {
    return store.getQuads(subject, predicate, null, graph).map((quad) => {
        return quad.object;
    });
}

/**
 * Splits a given RDF store into multiple stores, each containing a given maximum of quads.
 * This is useful for avoiding issues with large SPARQL updates that exceed the limits of some triple stores.
 *
 * @param store - The RDF store to split.
 * @param threshold - The maximum number of quads per store.
 * @returns An array of RDF stores, each containing up to the specified number of quads.
 */
export function splitStore(store: RdfStore, threshold: number): RdfStore[] {
    const stores: RdfStore[] = [];

    if (store.size < threshold) {
        stores.push(store);
    } else {
        const quads = store.getQuads();
        const bnSet = new Set<string>();
        let subStore = RdfStore.createDefault();

        // Split the store into chunks containing a maximum of quads given by `threshold`.
        for (let i = 0; i < quads.length; i++) {
            if (bnSet.has(`${quads[i].subject.value}${quads[i].predicate.value}${quads[i].object.value}${quads[i].graph.value}`)) {
                // Skip quads referencing blank nodes that have already been added to a subStore
                continue;
            }

            // Create a new subStore if the current one has reached the threshold
            if (subStore.size >= threshold) {
                stores.push(subStore);
                subStore = RdfStore.createDefault();
            }

            // Make sure all blank nodes quads are in the same store.
            if (quads[i].subject.termType === "BlankNode") {
                const subjectQuads = store.getQuads(quads[i].subject);
                const objectQuads = store.getQuads(null, null, quads[i].subject);

                [...subjectQuads, ...objectQuads].forEach((q) => {
                    subStore.addQuad(q);
                    bnSet.add(`${q.subject.value}${q.predicate.value}${q.object.value}${q.graph.value}`);
                });
            }
            if (quads[i].object.termType === "BlankNode") {
                const subjectQuads = store.getQuads(quads[i].object);
                const objectQuads = store.getQuads(null, null, quads[i].object);

                [...subjectQuads, ...objectQuads].forEach((q) => {
                    subStore.addQuad(q);
                    bnSet.add(`${q.subject.value}${q.predicate.value}${q.object.value}${q.graph.value}`);
                });
            }

            // Add the quad to the current subStore
            subStore.addQuad(quads[i]);
        }
        // Add the last subStore
        stores.push(subStore);
    }
    return stores;
}

export function sanitizeQuads(store: RdfStore): void {
    for (const q of store.getQuads()) {
        // There is an issue with triples like <a> <b> +30.
        // Virtuoso doesn't accept the implicit integer type including the + sign.
        if (q.object.termType === "Literal" && q.object.datatype.value === XSD.integer) {
            if (/\+\d+/.test(q.object.value) && q.object.value.startsWith("+")) {
                store.removeQuad(q);
                store.addQuad(df.quad(
                    q.subject,
                    q.predicate,
                    df.literal(q.object.value.substring(1), df.namedNode(XSD.integer)),
                    q.graph
                ));
            }
        }
    }
}

export async function doSPARQLRequest(query: string[], config: IngestConfig): Promise<void> {
    const logger = getLoggerFor("doSPARQLRequest");
    try {
        let queries: string[] = [];
        const jointQuery = query.join("\n");

        if (config.forVirtuoso && Buffer.byteLength(jointQuery, 'utf8') > 1e6) {
            // We need to split the query across multiple requests for Virtuoso,
            // when the query is too big (see https://community.openlinksw.com/t/virtuosoexception-sq199/1950).
            // We set 1MB as the maximum query size empirally, aiming to maximize the query size without hitting the limit.
            queries = query;
        }
        else {
            queries.push(jointQuery);
        }

        const timeout = config.measurePerformance?.queryTimeout || 1800; // Default to 30 minutes if not specified

        for (const q of queries) {
            logger.debug(`Executing SPARQL query: \n${q}`);
            const res = await fetch(config.graphStoreUrl!, {
                method: "POST",
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                },
                body: `update=${fixedEncodeURIComponent(q)}${config.accessToken ? `&access-token=${config.accessToken}` : ''}`,
                // Set the request timeout to accomodate for slow SPARQL engines.
                dispatcher: new Agent({
                    headersTimeout: timeout * 1000,
                    bodyTimeout: timeout * 1000,
                }),
            });
    
            if (!res.ok) {
                throw new Error(`HTTP request failed with code ${res.status} and message: \n${await res.text()}`);
            }
        }
    } catch (err: unknown) {
        logger.error(`Error while executing SPARQL request: ${(<Error>err).message} - ${(<Error>err).cause}`);
        throw err;
    }
}

function fixedEncodeURIComponent(str: string) {
    return encodeURIComponent(str).replace(/[!'()*]/g, function (c) {
        return '%' + c.charCodeAt(0).toString(16);
    });
}
